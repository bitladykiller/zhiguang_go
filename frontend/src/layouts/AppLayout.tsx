import { Edit3, GraduationCap, Home, MoonStar, Search, UserRound } from "lucide-react";
import { NavLink, useNavigate } from "react-router-dom";
import BrandMark from "@/components/BrandMark";
import Constellation from "@/components/decor/Constellation";
import Ornament from "@/components/decor/Ornament";
import SealMark from "@/components/decor/SealMark";
import Button from "@/components/ui/Button";
import { useAuth } from "@/features/auth/AuthProvider";
import styles from "@/layouts/AppLayout.module.css";

const navItems = [
  { to: "/", label: "首页", en: "ATLAS", icon: Home },
  { to: "/search", label: "搜索", en: "SEEK", icon: Search },
  { to: "/create", label: "创作", en: "WRITE", icon: Edit3 },
  { to: "/learn", label: "学习", en: "STUDY", icon: GraduationCap },
  { to: "/profile", label: "我的", en: "SELF", icon: UserRound }
] as const;

const todayLabel = new Intl.DateTimeFormat("zh-CN", {
  month: "long",
  day: "numeric",
  weekday: "long"
}).format(new Date());

const AppLayout = ({ children }: { children: React.ReactNode }) => {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  return (
    <div className={styles.shell}>
      <aside className={styles.sidebar}>
        <BrandMark />
        <Ornament className={styles.sidebarOrnament} />
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
                <i className={styles.navGlyph}>
                  <Icon size={18} strokeWidth={1.9} />
                </i>
                <span className={styles.navLabel}>{item.label}</span>
                <small className={styles.navEn}>{item.en}</small>
              </NavLink>
            );
          })}
        </nav>

        <div className={styles.sidebarPanel}>
          <Constellation className={styles.panelSky} />
          <div className={styles.panelHead}>
            <MoonStar size={17} strokeWidth={1.8} />
            <strong>今夜自习室</strong>
          </div>
          <p>长夜有灯。把一条知识，沉淀成可复用的资产。</p>
          <SealMark size="sm" className={styles.panelSeal} />
        </div>
      </aside>

      <div className={styles.main}>
        <header className={styles.topbar}>
          <div className={styles.topbarTitle}>
            <span className={styles.eyebrow}>ZHIGUANG · KNOWLEDGE ATLAS</span>
            <strong>夜读知识星图</strong>
          </div>
          <div className={styles.topbarSide}>
            <time className={styles.date}>{todayLabel} · 宜夜读</time>
            {user ? (
              <div className={styles.userBox}>
                <span className={styles.userAvatar}>{user.name.slice(0, 1)}</span>
                <div className={styles.userMeta}>
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
          </div>
        </header>
        <main className={styles.content}>{children}</main>
        <footer className={styles.footer}>
          <Ornament label="知 光 · 星 夜 书 院" />
          <p>以夜为幕，以知为光 —— 知光 ZhiGuang</p>
        </footer>
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
