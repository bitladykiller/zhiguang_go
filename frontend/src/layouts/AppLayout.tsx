import { BookOpen, Compass, Edit3, GraduationCap, Home, Search, UserRound } from "lucide-react";
import { NavLink, useNavigate } from "react-router-dom";
import BrandMark from "@/components/BrandMark";
import Button from "@/components/ui/Button";
import { useAuth } from "@/features/auth/AuthProvider";
import styles from "@/layouts/AppLayout.module.css";

const navItems = [
  { to: "/", label: "首页", icon: Home },
  { to: "/search", label: "搜索", icon: Search },
  { to: "/create", label: "创作", icon: Edit3 },
  { to: "/learn", label: "学习", icon: GraduationCap },
  { to: "/profile", label: "我的", icon: UserRound }
] as const;

const AppLayout = ({ children }: { children: React.ReactNode }) => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  return (
    <div className={styles.shell}>
      <aside className={styles.sidebar}>
        <BrandMark />
        <nav className={styles.nav} aria-label="主导航">
          {navItems.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.to === "/"}
                className={({ isActive }) => (isActive ? `${styles.navLink} ${styles.active}` : styles.navLink)}
              >
                <Icon size={19} strokeWidth={1.9} />
                <span>{item.label}</span>
              </NavLink>
            );
          })}
        </nav>
        <div className={styles.sidebarPanel}>
          <BookOpen size={20} />
          <strong>每日构建</strong>
          <span>把一条知识沉淀成可复用资产。</span>
        </div>
      </aside>

      <div className={styles.main}>
        <header className={styles.topbar}>
          <div>
            <span className={styles.eyebrow}>Knowledge Operating System</span>
            <strong>精致知识工作台</strong>
          </div>
          {user ? (
            <div className={styles.userBox}>
              <span>{user.name.slice(0, 1)}</span>
              <div>
                <strong>{user.name}</strong>
                <small>{user.title}</small>
              </div>
              <Button variant="ghost" onClick={logout}>
                退出
              </Button>
            </div>
          ) : (
            <Button variant="primary" onClick={() => navigate("/login")}>
              登录
            </Button>
          )}
        </header>
        <main className={styles.content}>{children}</main>
      </div>

      <nav className={styles.mobileNav} aria-label="移动端主导航">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) => (isActive ? `${styles.mobileLink} ${styles.mobileActive}` : styles.mobileLink)}
            >
              <Icon size={20} strokeWidth={1.9} />
              <span>{item.label}</span>
            </NavLink>
          );
        })}
      </nav>
    </div>
  );
};

export default AppLayout;
