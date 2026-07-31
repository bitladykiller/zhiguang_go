import { createContext, useContext, useMemo, useState } from "react";
import { authService } from "@/services/authService";
import type { LoginInput, RegisterInput, VerificationScene } from "@/types/auth";
import type { User } from "@/types/domain";

type AuthContextValue = {
  user: User | null;
  login: (input: LoginInput) => Promise<User>;
  register: (input: RegisterInput) => Promise<User>;
  sendCode: (account: string, scene: VerificationScene) => Promise<number>;
  logout: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [user, setUser] = useState<User | null>(() => authService.current());

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      async login(input) {
        const nextUser = await authService.login(input);
        setUser(nextUser);
        return nextUser;
      },
      async register(input) {
        const nextUser = await authService.register(input);
        setUser(nextUser);
        return nextUser;
      },
      async sendCode(account, scene) {
        const resp = await authService.sendCode(account, scene);
        return resp.expire_seconds;
      },
      async logout() {
        try {
          await authService.logout();
        } finally {
          setUser(null);
        }
      }
    }),
    [user]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = () => {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
};
