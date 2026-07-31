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

const RegisterPage = () => {
  const [account, setAccount] = useState("new@zhiguang.local");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [sendingCode, setSendingCode] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const { register, sendCode } = useAuth();
  const navigate = useNavigate();

  const requestCode = async () => {
    setError("");
    setMessage("");
    if (!account.trim()) {
      setError("请先填写邮箱或手机号。");
      return;
    }

    setSendingCode(true);
    try {
      const expireSeconds = await sendCode(account.trim(), "REGISTER");
      setMessage(`验证码已发送，${expireSeconds} 秒内有效。`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "验证码发送失败，请稍后重试。");
    } finally {
      setSendingCode(false);
    }
  };

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError("");
    setMessage("");

    if (!account.trim()) {
      setError("请输入邮箱或手机号。");
      return;
    }
    if (!code.trim()) {
      setError("请输入验证码。");
      return;
    }
    if (!password) {
      setError("请设置密码，后续可直接使用密码登录。");
      return;
    }

    setSubmitting(true);
    try {
      await register({ account: account.trim(), password, code: code.trim() });
      navigate("/profile");
    } catch (err) {
      setError(err instanceof Error ? err.message : "注册失败，请稍后重试。");
    } finally {
      setSubmitting(false);
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
        <span className={auth.visualMark}>点一盏灯 · 落一颗星</span>

        <div className={clsx("rise", "d1")}>
          <BrandMark />
        </div>

        <div className={auth.visualText}>
          <span className={clsx(styles.kicker, "rise", "d2")}>始于今夜 · START BUILDING</span>
          <h1 className={clsx("rise", "d3")}>
            把你的经验，铸成
            <em className="gilded">可复用的光</em>。
          </h1>
          <p className={clsx("rise", "d4")}>
            注册流程已接入后端验证码与令牌签发，成功后会直接进入登录态。
          </p>
          <ul className={clsx(auth.points, "rise", "d5")}>
            <li>发布结构清晰、可被检索与收藏的知文。</li>
            <li>用学习星轨追踪自己的成长路径。</li>
            <li>与工程师、研究者和产品学习者同院夜读。</li>
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
            <h2>创建账号</h2>
            <p>进入知光，开始沉淀高质量内容。</p>
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
                placeholder="至少 8 位，包含字母和数字"
              />
            </label>
            <label className={styles.field}>
              <span>验证码 · CODE</span>
              <input className={styles.input} value={code} onChange={(event) => setCode(event.target.value)} />
            </label>
            {message ? <div className={styles.message}>{message}</div> : null}
            {error ? <div className={styles.error}>{error}</div> : null}
            <Button type="button" variant="secondary" onClick={requestCode} disabled={sendingCode}>
              {sendingCode ? "发送中..." : "发送验证码"}
            </Button>
            <Button type="submit" disabled={submitting}>
              {submitting ? "正在注册..." : "落笔为星，注册进入"}
            </Button>
          </div>
          <p className={auth.linkText}>
            已有账号？<Link to="/login">去登录</Link>
          </p>
        </form>
      </section>
    </div>
  );
};

export default RegisterPage;
