import clsx from "clsx";
import { useNavigate } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import Constellation from "@/components/decor/Constellation";
import MetricCard from "@/components/MetricCard";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import Tag from "@/components/ui/Tag";
import { useAuth } from "@/features/auth/AuthProvider";
import AppLayout from "@/layouts/AppLayout";
import { mockArticles } from "@/services/contentService";
import styles from "@/pages/PageStyles.module.css";
import profile from "@/pages/ProfilePage.module.css";

const ProfilePage = () => {
  const { user } = useAuth();
  const navigate = useNavigate();

  const showcase = mockArticles.slice(0, 3);
  const totalLikes = showcase.reduce((sum, item) => sum + item.likes, 0);
  const totalFavorites = showcase.reduce((sum, item) => sum + item.favorites, 0);

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={clsx(styles.pageHead, "rise", "d1")}>
          <div className={styles.pageHeadText}>
            <span className={styles.kicker}>灯主 · SELF</span>
            <h1>{user ? `${user.name} 的知识灯房` : "登录后，点亮你的知识灯房。"}</h1>
            <p>展示创作者定位、技能标签、发布内容和收藏沉淀，让个人主页像一份可信的知识档案。</p>
          </div>
          {!user ? (
            <div className={styles.headerActions}>
              <Button onClick={() => navigate("/login")}>登录</Button>
            </div>
          ) : null}
        </section>

        {user ? (
          <>
            <section className={clsx(profile.identity, "bloom", "d2")}>
              <Constellation className={profile.identitySky} />
              <span className={profile.bigAvatar}>{user.name.slice(0, 1)}</span>
              <div className={profile.idText}>
                <span className={profile.idTitle}>{user.title}</span>
                <h2>{user.name}</h2>
                <div className={profile.skillRow}>
                  {user.skills.map((skill) => (
                    <Tag key={skill} tone="gold">
                      {skill}
                    </Tag>
                  ))}
                </div>
                {user.email ? <span className={profile.idMail}>{user.email}</span> : null}
              </div>
            </section>

            <div className={clsx(profile.statRow, "rise", "d3")}>
              <MetricCard label="代表知文 / POSTS" value={String(showcase.length)} caption="灯下最能代表你的三篇沉淀" />
              <MetricCard label="获赞 / LIKES" value={totalLikes.toLocaleString()} caption="来自代表知文的累计点亮" />
              <MetricCard label="被收藏 / SAVED" value={totalFavorites.toLocaleString()} caption="被其他夜读者收入书架的次数" />
            </div>

            <section className={clsx("rise", "d4")}>
              <header className={styles.sectionHead}>
                <div className={styles.sectionTitle}>
                  <span className={styles.chapter}>藏卷 · WORKS</span>
                  <h2>代表知文</h2>
                  <p>个人主页默认展示最近沉淀内容。</p>
                </div>
                <span className={styles.sectionRule} aria-hidden="true" />
              </header>
              <div className={styles.grid}>
                {showcase.map((article) => (
                  <ArticleCard key={article.id} article={article} />
                ))}
              </div>
            </section>
          </>
        ) : (
          <EmptyState
            title="尚未登录"
            description="登录后可查看你的发布、收藏和学习进度。"
            action={{ label: "去登录", onClick: () => navigate("/login") }}
          />
        )}
      </div>
    </AppLayout>
  );
};

export default ProfilePage;
