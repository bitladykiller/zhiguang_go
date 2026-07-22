import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import BrandMark from "@/components/BrandMark";
import Button from "@/components/ui/Button";
import { useAuth } from "@/features/auth/AuthProvider";
import styles from "@/pages/PageStyles.module.css";

const RegisterPage = () => {
  const [account, setAccount] = useState("new@zhiguang.local");
  const [name, setName] = useState("新创作者");
  const { login } = useAuth();
  const navigate = useNavigate();

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    login(account || name);
    navigate("/profile");
  };

  return (
    <div className={styles.authPage}>
      <section className={styles.authVisual}>
        <BrandMark />
        <div className={styles.headerText}>
          <span className={styles.kicker}>Start building</span>
          <h1>把你的经验变成可复用资产。</h1>
          <p>注册流程先以本地状态完成交互闭环，后续可接短信验证码、邮箱验证和用户资料 API。</p>
        </div>
      </section>
      <section className={styles.authCard}>
        <div>
          <h2>创建账号</h2>
          <p className={styles.helper}>进入知光，开始沉淀高质量内容。</p>
        </div>
        <form className={styles.authForm} onSubmit={submit}>
          <label className={styles.field}>
            <span>昵称</span>
            <input className={styles.input} value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          <label className={styles.field}>
            <span>邮箱</span>
            <input className={styles.input} value={account} onChange={(event) => setAccount(event.target.value)} />
          </label>
          <Button type="submit">注册并进入</Button>
        </form>
        <p className={styles.linkText}>
          已有账号？<Link to="/login">去登录</Link>
        </p>
      </section>
    </div>
  );
};

export default RegisterPage;
