import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import BrandMark from "@/components/BrandMark";
import Button from "@/components/ui/Button";
import { useAuth } from "@/features/auth/AuthProvider";
import styles from "@/pages/PageStyles.module.css";

const LoginPage = () => {
  const [account, setAccount] = useState("creator@zhiguang.local");
  const [password, setPassword] = useState("");
  const { login } = useAuth();
  const navigate = useNavigate();

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    login(account);
    navigate("/profile");
  };

  return (
    <div className={styles.authPage}>
      <section className={styles.authVisual}>
        <BrandMark />
        <div className={styles.headerText}>
          <span className={styles.kicker}>Welcome back</span>
          <h1>回到你的知识工作台。</h1>
          <p>当前登录页使用本地状态模拟登录，后续接入 Go 后端认证接口时不需要重做页面结构。</p>
        </div>
      </section>
      <section className={styles.authCard}>
        <div>
          <h2>登录</h2>
          <p className={styles.helper}>继续管理发布、收藏和学习路径。</p>
        </div>
        <form className={styles.authForm} onSubmit={submit}>
          <label className={styles.field}>
            <span>邮箱或手机号</span>
            <input className={styles.input} value={account} onChange={(event) => setAccount(event.target.value)} />
          </label>
          <label className={styles.field}>
            <span>密码</span>
            <input className={styles.input} type="password" value={password} onChange={(event) => setPassword(event.target.value)} placeholder="任意输入即可演示" />
          </label>
          <Button type="submit">登录</Button>
        </form>
        <p className={styles.linkText}>
          还没有账号？<Link to="/register">创建一个</Link>
        </p>
      </section>
    </div>
  );
};

export default LoginPage;
