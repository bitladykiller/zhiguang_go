import clsx from "clsx";
import { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import BrandMark from "@/components/BrandMark";
import Constellation from "@/components/decor/Constellation";
import SealMark from "@/components/decor/SealMark";
import Button from "@/components/ui/Button";
import { useAuth } from "@/features/auth/AuthProvider";
import styles from "@/pages/PageStyles.module.css";
import auth from "@/pages/AuthPages.module.css";

const LoginPage = () => {
  const [account, setAccount] = useState("creator@zhiguang.local");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const { login } = useAuth();
  const navigate = useNavigate();

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError("");

    if (!account.trim()) {
      setError("请输入邮箱或手机号。");
      return;
    }
    if (!password) {
      setError("请输入密码。");
      return;
    }

    setLoading(true);
    try {
      await login({ account: account.trim(), password });
      navigate("/profile");
    } catch (err) {
      setError(err instanceof Error ? err.message : "登录失败，请稍后重试。");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className={auth.authPage}>
      <section className={auth.visual}>
        <span className={auth.rings} aria-hidden="true">
          <i />
          <i />
        </span>
        <Constellation className={auth.sky} />
        <span className={auth.visualMark}>长夜孤读 · 有灯有光</span>

        <div className={clsx("rise", "d1")}>
          <BrandMark />
        </div>

        <div className={auth.visualText}>
          <span className={clsx(styles.kicker, "rise", "d2")}>夜航回港 · WELCOME BACK</span>
          <h1 className={clsx("rise", "d3")}>
            回到你的
            <em className="gilded">知识星图</em>。
          </h1>
          <p className={clsx("rise", "d4")}>
            当前登录页已接入 Go 后端认证接口，登录成功后会保存 access token 与 refresh token。
          </p>
          <ul className={clsx(auth.points, "rise", "d5")}>
            <li>继续管理你的发布、收藏与学习星轨。</li>
            <li>所有知文按主题、标签与作者可检索。</li>
            <li>阅读进度与灯下笔记，随时可以接回后端。</li>
          </ul>
        </div>

        <div className={clsx(auth.visualFoot, "rise", "d6")}>
          <SealMark size="sm" />
          <span>ZHIGUANG · CELESTIAL ACADEMY</span>
        </div>
      </section>

      <section className={auth.cardWrap}>
        <form className={clsx(auth.card, "bloom", "d3")} onSubmit={submit}>
          <div className={auth.cardHead}>
            <h2>登录</h2>
            <p>继续管理发布、收藏和学习路径。</p>
          </div>
          <div className={auth.form}>
            <label className={styles.field}>
              <span>邮箱或手机号 · ACCOUNT</span>
              <input className={styles.input} value={account} onChange={(event) => setAccount(event.target.value)} />
            </label>
            <label className={styles.field}>
              <span>密码 · PASSWORD</span>
              <input
                className={styles.input}
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                placeholder="请输入后端账号密码"
              />
            </label>
            {error ? <div className={styles.error}>{error}</div> : null}
            <Button type="submit" disabled={loading}>
              {loading ? "正在校验..." : "提灯入院"}
            </Button>
          </div>
          <p className={auth.linkText}>
            还没有账号？<Link to="/register">创建一个</Link>
          </p>
        </form>
      </section>
    </div>
  );
};

export default LoginPage;
