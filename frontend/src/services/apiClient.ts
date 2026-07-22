const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "";

export class ApiError extends Error {
  constructor(
    message: string,
    public readonly status?: number
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export const requestJson = async <T>(path: string, init?: RequestInit): Promise<T> => {
  const resp = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    },
    ...init
  });

  if (!resp.ok) {
    throw new ApiError(`HTTP ${resp.status}`, resp.status);
  }

  return (await resp.json()) as T;
};
