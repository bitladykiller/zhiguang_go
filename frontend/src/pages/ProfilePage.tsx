import { useNavigate } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import Tag from "@/components/ui/Tag";
import { useAuth } from "@/features/auth/AuthProvider";
import AppLayout from "@/layouts/AppLayout";
import { mockArticles } from "@/services/contentService";
import styles from "@/pages/PageStyles.module.css";

const ProfilePage = () => {
  const { user } = useAuth();
  const navigate = useNavigate();

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={styles.header}>
          <div className={styles.headerText}>
            <span className={styles.kicker}>个人主页</span>
            <h1>{user ? `${user.name} 的知识资产` : "登录后管理你的知识资产。"}</h1>
            <p>展示创作者定位、技能标签、发布内容和收藏沉淀，让个人主页像一个可信的知识档案。</p>
          </div>
          {!user ? (
            <div className={styles.headerActions}>
              <Button onClick={() => navigate("/login")}>登录</Button>
            </div>
          ) : null}
        </section>

        {user ? (
          <>
            <section className={styles.section}>
              <div className={styles.sectionHead}>
                <div className={styles.sectionTitle}>
                  <h2>{user.name}</h2>
                  <p>{user.title}</p>
                </div>
              </div>
              <div className={styles.headerActions}>
                {user.skills.map((skill) => (
                  <Tag key={skill} tone="gold">
                    {skill}
                  </Tag>
                ))}
              </div>
            </section>
            <section className={styles.section}>
              <div className={styles.sectionHead}>
                <div className={styles.sectionTitle}>
                  <h2>代表知文</h2>
                  <p>个人主页默认展示最近沉淀内容。</p>
                </div>
              </div>
              <div className={styles.grid}>
                {mockArticles.slice(0, 3).map((article) => (
                  <ArticleCard key={article.id} article={article} />
                ))}
              </div>
            </section>
          </>
        ) : (
          <EmptyState title="尚未登录" description="登录后可查看你的发布、收藏和学习进度。" action={{ label: "去登录", onClick: () => navigate("/login") }} />
        )}
      </div>
    </AppLayout>
  );
};

export default ProfilePage;
