import type { StoredTokenSession, TokenResponse } from "@/types/auth";

export const TOKEN_STORAGE_KEY = "zhiguang.auth";

const isStoredTokenSession = (value: unknown): value is StoredTokenSession => {
  if (!value || typeof value !== "object") return false;
  const session = value as Partial<StoredTokenSession>;
  return (
    typeof session.accessToken === "string" &&
    typeof session.accessTokenExpiresAt === "string" &&
    typeof session.refreshToken === "string" &&
    typeof session.refreshTokenExpiresAt === "string"
  );
};

export const toStoredTokenSession = (token: TokenResponse): StoredTokenSession => ({
  accessToken: token.access_token,
  accessTokenExpiresAt: token.access_token_expires_at,
  refreshToken: token.refresh_token,
  refreshTokenExpiresAt: token.refresh_token_expires_at
});

export const getTokenSession = (): StoredTokenSession | null => {
  const raw = localStorage.getItem(TOKEN_STORAGE_KEY);
  if (!raw) return null;

  try {
    const parsed = JSON.parse(raw) as unknown;
    if (!isStoredTokenSession(parsed)) {
      localStorage.removeItem(TOKEN_STORAGE_KEY);
      return null;
    }
    return parsed;
  } catch {
    localStorage.removeItem(TOKEN_STORAGE_KEY);
    return null;
  }
};

export const saveTokenSession = (token: TokenResponse | StoredTokenSession): StoredTokenSession => {
  const session = "access_token" in token ? toStoredTokenSession(token) : token;
  localStorage.setItem(TOKEN_STORAGE_KEY, JSON.stringify(session));
  return session;
};

export const clearTokenSession = (): void => {
  localStorage.removeItem(TOKEN_STORAGE_KEY);
};

export const getAccessToken = (): string | null => getTokenSession()?.accessToken ?? null;

export const getRefreshToken = (): string | null => getTokenSession()?.refreshToken ?? null;
