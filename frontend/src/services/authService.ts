import type { User } from "@/types/domain";
import { requestJson } from "@/services/apiClient";
import { clearTokenSession, getTokenSession, saveTokenSession } from "@/services/tokenStorage";
import type {
  AuthResponse,
  AuthUserResponse,
  IdentifierType,
  LoginInput,
  RegisterInput,
  SendCodeResponse,
  VerificationScene
} from "@/types/auth";

const STORAGE_KEY = "zhiguang.user";

export const fallbackUser: User = {
  id: "local-user",
  name: "知光创作者",
  title: "知识工程实践者",
  email: "creator@zhiguang.local",
  skills: ["AI Agent", "Go 后端", "知识管理"]
};

const inferIdentifierType = (account: string): IdentifierType => (account.includes("@") ? "EMAIL" : "PHONE");

const parseSkills = (tagsJson?: string | null): string[] => {
  if (!tagsJson) return fallbackUser.skills;

  try {
    const parsed = JSON.parse(tagsJson) as unknown;
    if (Array.isArray(parsed)) {
      const tags = parsed.map(String).filter(Boolean);
      return tags.length > 0 ? tags : fallbackUser.skills;
    }
  } catch {
    const tags = tagsJson
      .split(/[,，\s]+/)
      .map((item) => item.trim())
      .filter(Boolean);
    return tags.length > 0 ? tags : fallbackUser.skills;
  }

  return fallbackUser.skills;
};

const mapAuthUser = (apiUser: AuthUserResponse, account?: string): User => ({
  id: String(apiUser.id),
  name: apiUser.nickname || fallbackUser.name,
  title: apiUser.bio || apiUser.school || apiUser.zg_id || fallbackUser.title,
  email: account?.includes("@") ? account : undefined,
  avatar: apiUser.avatar ?? undefined,
  skills: parseSkills(apiUser.tags_json)
});

const saveUser = (user: User): User => {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(user));
  return user;
};

const saveAuthResponse = (resp: AuthResponse, account?: string): User => {
  const user = mapAuthUser(resp.user, account);
  saveTokenSession(resp.token);
  return saveUser(user);
};

const clearUser = (): void => {
  localStorage.removeItem(STORAGE_KEY);
};

export const authService = {
  current(): User | null {
    if (!getTokenSession()) {
      clearUser();
      return null;
    }

    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    try {
      return JSON.parse(raw) as User;
    } catch {
      clearUser();
      clearTokenSession();
      return null;
    }
  },

  async sendCode(account: string, scene: VerificationScene): Promise<SendCodeResponse> {
    return requestJson<SendCodeResponse>("/api/v1/auth/send-code", {
      method: "POST",
      auth: false,
      body: JSON.stringify({
        identifier: account,
        identifier_type: inferIdentifierType(account),
        scene
      })
    });
  },

  async login(input: LoginInput): Promise<User> {
    const data = await requestJson<AuthResponse>("/api/v1/auth/login", {
      method: "POST",
      auth: false,
      body: JSON.stringify({
        identifier: input.account,
        identifier_type: inferIdentifierType(input.account),
        password: input.password
      })
    });

    return saveAuthResponse(data, input.account);
  },

  async register(input: RegisterInput): Promise<User> {
    const data = await requestJson<AuthResponse>("/api/v1/auth/register", {
      method: "POST",
      auth: false,
      body: JSON.stringify({
        identifier: input.account,
        identifier_type: inferIdentifierType(input.account),
        password: input.password,
        code: input.code,
        agree_terms: true
      })
    });

    return saveAuthResponse(data, input.account);
  },

  async logout(): Promise<void> {
    const refreshToken = getTokenSession()?.refreshToken;
    try {
      if (refreshToken) {
        await requestJson<{ message: string }>("/api/v1/auth/logout", {
          method: "POST",
          body: JSON.stringify({ refresh_token: refreshToken })
        });
      }
    } catch {
      // 本地登出必须成功；服务端吊销失败时只保留后端日志排查，不阻断用户退出。
    } finally {
      clearUser();
      clearTokenSession();
    }
  }
};
