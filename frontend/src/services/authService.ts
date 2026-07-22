import type { User } from "@/types/domain";

const STORAGE_KEY = "zhiguang.user";

export const fallbackUser: User = {
  id: "local-user",
  name: "知光创作者",
  title: "知识工程实践者",
  email: "creator@zhiguang.local",
  skills: ["AI Agent", "Go 后端", "知识管理"]
};

export const authService = {
  current(): User | null {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    try {
      return JSON.parse(raw) as User;
    } catch {
      localStorage.removeItem(STORAGE_KEY);
      return null;
    }
  },

  login(account: string): User {
    const user = {
      ...fallbackUser,
      email: account.includes("@") ? account : fallbackUser.email
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(user));
    return user;
  },

  logout(): void {
    localStorage.removeItem(STORAGE_KEY);
  }
};
