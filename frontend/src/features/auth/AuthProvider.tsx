import { createContext, useContext, useMemo, useState } from "react";
import { authService } from "@/services/authService";
import type { User } from "@/types/domain";

type AuthContextValue = {
  user: User | null;
  login: (account: string) => void;
  logout: () => void;
};

const AuthContext = createContext<AuthContextValue | null>(null);

export const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [user, setUser] = useState<User | null>(() => authService.current());

  const value = useMemo<AuthContextValue>(
    () => ({
      user,
      login(account) {
        setUser(authService.login(account));
      },
      logout() {
        authService.logout();
        setUser(null);
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
