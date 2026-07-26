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
  const [name, setName] = useState("新创作者");
  const { login } = useAuth();
  const navigate = useNavigate();

  const submit = (event: React.FormEvent) => {
    event.preventDefault();
    login(account || name);
    navigate("/profile");
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
            注册流程先以本地状态完成交互闭环，后续可接短信验证码、邮箱验证和用户资料 API。
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
              <span>昵称 · NAME</span>
              <input className={styles.input} value={name} onChange={(event) => setName(event.target.value)} />
            </label>
            <label className={styles.field}>
              <span>邮箱 · EMAIL</span>
              <input className={styles.input} value={account} onChange={(event) => setAccount(event.target.value)} />
            </label>
            <Button type="submit">落笔为星，注册进入</Button>
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
