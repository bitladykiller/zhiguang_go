import type { User } from "@/types/domain";

export type IdentifierType = "PHONE" | "EMAIL";

export type VerificationScene = "REGISTER" | "LOGIN" | "RESET_PASSWORD";

export type ApiResponse<T> = {
  code: number;
  message: string;
  data?: T;
};

export type AuthUserResponse = {
  id: number;
  nickname: string;
  avatar?: string | null;
  phone?: string | null;
  zg_id?: string | null;
  birthday?: string | null;
  school?: string | null;
  bio?: string | null;
  gender?: string | null;
  tags_json?: string | null;
};

export type TokenResponse = {
  access_token: string;
  access_token_expires_at: string;
  refresh_token: string;
  refresh_token_expires_at: string;
};

export type AuthResponse = {
  user: AuthUserResponse;
  token: TokenResponse;
};

export type SendCodeResponse = {
  identifier: string;
  scene: VerificationScene;
  expire_seconds: number;
};

export type StoredTokenSession = {
  accessToken: string;
  accessTokenExpiresAt: string;
  refreshToken: string;
  refreshTokenExpiresAt: string;
};

export type LoginInput = {
  account: string;
  password: string;
};

export type RegisterInput = {
  account: string;
  password: string;
  code: string;
};

export type AuthSession = {
  user: User;
  token: StoredTokenSession;
};
