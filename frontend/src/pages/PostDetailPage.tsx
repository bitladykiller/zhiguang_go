import clsx from "clsx";
import { ArrowLeft, Bookmark, Calendar, Clock, Eye, Heart } from "lucide-react";
import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import Ornament from "@/components/decor/Ornament";
import SealMark from "@/components/decor/SealMark";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import Tag from "@/components/ui/Tag";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";
import detail from "@/pages/PostDetailPage.module.css";

const PostDetailPage = () => {
  const { id } = useParams<{ id: string }>();
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(true);
  const [liked, setLiked] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    let cancelled = false;
    if (!id) return;
    void contentService.detail(id).then((item) => {
      if (!cancelled) {
        setArticle(item);
        setLoading(false);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [id]);

  return (
    <AppLayout>
      <div className={styles.page}>
        {loading ? (
          <EmptyState busy title="正在展卷" description="正在读取正文与作者信息。" />
        ) : !article ? (
          <EmptyState title="知文不存在" description="这篇内容可能已删除或暂未公开。" />
        ) : (
          <>
            <Link to="/" className={clsx(detail.backLink, "rise", "d1")}>
              <ArrowLeft size={14} />
              返回星图
            </Link>

            <section className={clsx(detail.heroMedia, "bloom", "d1")}>
              <img src={article.cover} alt="" />
              <SealMark size="md" className={detail.heroSeal} />
              <div className={detail.heroText}>
                <div className={detail.heroTags}>
                  <Tag tone="gold">{article.level}</Tag>
                  {article.tags.map((tag) => (
                    <Tag key={tag}>{tag}</Tag>
                  ))}
                </div>
                <h1>{article.title}</h1>
                <div className={detail.heroMeta}>
                  {article.publishedAt ? (
                    <span>
                      <Calendar size={13} style={{ verticalAlign: -2, marginRight: 6 }} />
                      {article.publishedAt}
                    </span>
                  ) : null}
                  <span>
                    <Clock size={13} style={{ verticalAlign: -2, marginRight: 6 }} />
                    约 {article.minutes} 分钟
                  </span>
                  <span>
                    <Eye size={13} style={{ verticalAlign: -2, marginRight: 6 }} />
                    {article.reads.toLocaleString()} 次夜读
                  </span>
                </div>
              </div>
            </section>

            <div className={detail.bodyGrid}>
              <article className={clsx(detail.article, "rise", "d3")}>
                {article.summary ? <blockquote className={detail.lead}>{article.summary}</blockquote> : null}
                <Ornament label="正 文" />
                <p className={detail.prose}>{article.content}</p>
                <div className={detail.signature}>
                  <em>—— {article.author.name} · 谨识</em>
                  <SealMark size="sm" />
                </div>
                <Ornament />
              </article>

              <aside className={clsx(detail.rail, "rise", "d4")}>
                <div className={detail.authorCard}>
                  <div className={detail.authorRow}>
                    <span className={detail.authorAvatar}>{article.author.name.slice(0, 1)}</span>
                    <div>
                      <strong>{article.author.name}</strong>
                      <small>{article.author.title}</small>
                    </div>
                  </div>
                  {article.author.skills.length > 0 ? (
                    <div className={styles.headerActions}>
                      {article.author.skills.map((skill) => (
                        <Tag key={skill} tone="gold">
                          {skill}
                        </Tag>
                      ))}
                    </div>
                  ) : null}
                </div>

                <div className={styles.insightList}>
                  <div className={styles.insight}>
                    <span>
                      <Clock size={17} />
                    </span>
                    <div>
                      <strong>{article.minutes} 分钟</strong>
                      <p>建议阅读时间</p>
                    </div>
                  </div>
                  <div className={styles.insight}>
                    <span>
                      <Eye size={17} />
                    </span>
                    <div>
                      <strong>{article.reads.toLocaleString()}</strong>
                      <p>累计阅读</p>
                    </div>
                  </div>
                </div>

                <div className={detail.railActions}>
                  <Button
                    variant={saved ? "primary" : "secondary"}
                    icon={<Bookmark size={17} />}
                    onClick={() => setSaved((prev) => !prev)}
                  >
                    {saved ? "已收藏" : "收藏"} · {(article.favorites + (saved ? 1 : 0)).toLocaleString()}
                  </Button>
                  <Button
                    variant={liked ? "danger" : "ghost"}
                    icon={<Heart size={17} />}
                    onClick={() => setLiked((prev) => !prev)}
                  >
                    {liked ? "已点亮" : "点亮此文"} · {(article.likes + (liked ? 1 : 0)).toLocaleString()}
                  </Button>
                </div>
              </aside>
            </div>
          </>
        )}
      </div>
    </AppLayout>
  );
};

export default PostDetailPage;
