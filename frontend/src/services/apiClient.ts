import { clearTokenSession, getAccessToken, getRefreshToken, saveTokenSession } from "@/services/tokenStorage";
import type { ApiResponse, AuthResponse } from "@/types/auth";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status?: number,
    public readonly code?: number
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type RequestJsonInit = RequestInit & {
  auth?: boolean;
  retryOnUnauthorized?: boolean;
};

const isApiResponse = (value: unknown): value is ApiResponse<unknown> => {
  if (!value || typeof value !== "object") return false;
  const resp = value as Partial<ApiResponse<unknown>>;
  return typeof resp.code === "number" && typeof resp.message === "string";
};

const parseJsonBody = async (resp: Response): Promise<unknown> => {
  if (resp.status === 204) return null;

  const text = await resp.text();
  if (!text) return null;

  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new ApiError("响应不是合法 JSON", resp.status);
  }
};

const unwrapApiResponse = <T>(body: unknown, status?: number): T => {
  if (!isApiResponse(body)) return body as T;

  if (body.code !== 0) {
    throw new ApiError(body.message || "请求失败", status, body.code);
  }

  return body.data as T;
};

const buildHeaders = (init: RequestJsonInit, accessToken: string | null): HeadersInit => ({
  "Content-Type": "application/json",
  ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
  ...(init.headers ?? {})
});

const refreshTokenPair = async (): Promise<boolean> => {
  const refreshToken = getRefreshToken();
  if (!refreshToken) return false;

  const resp = await fetch(`${API_BASE}/api/v1/auth/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: refreshToken })
  });

  const body = await parseJsonBody(resp);
  if (!resp.ok) {
    clearTokenSession();
    return false;
  }

  try {
    const data = unwrapApiResponse<AuthResponse>(body, resp.status);
    saveTokenSession(data.token);
    return true;
  } catch {
    clearTokenSession();
    return false;
  }
};

const sendJson = async <T>(path: string, init: RequestJsonInit): Promise<T> => {
  const { auth = true, retryOnUnauthorized = true, ...requestInit } = init;
  const accessToken = auth ? getAccessToken() : null;

  const resp = await fetch(`${API_BASE}${path}`, {
    ...requestInit,
    headers: buildHeaders(init, accessToken)
  });

  if (resp.status === 401 && auth && retryOnUnauthorized && !path.includes("/auth/refresh")) {
    const refreshed = await refreshTokenPair();
    if (refreshed) {
      return sendJson<T>(path, { ...init, retryOnUnauthorized: false });
    }
  }

  const body = await parseJsonBody(resp);

  if (!resp.ok) {
    if (isApiResponse(body)) {
      throw new ApiError(body.message || `HTTP ${resp.status}`, resp.status, body.code);
    }
    throw new ApiError(`HTTP ${resp.status}`, resp.status);
  }

  return unwrapApiResponse<T>(body, resp.status);
};

export const requestJson = async <T>(path: string, init: RequestJsonInit = {}): Promise<T> => sendJson<T>(path, init);
