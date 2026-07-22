import { Bookmark, Clock, Eye, Heart } from "lucide-react";
import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import Tag from "@/components/ui/Tag";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";

const PostDetailPage = () => {
  const { id } = useParams<{ id: string }>();
  const [article, setArticle] = useState<Article | null>(null);
  const [loading, setLoading] = useState(true);

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
      {loading ? (
        <EmptyState title="正在加载知文" description="正在读取正文和作者信息。" />
      ) : !article ? (
        <EmptyState title="知文不存在" description="这篇内容可能已删除或暂未公开。" />
      ) : (
        <article className={styles.detailPanel}>
          <div className={styles.detailHero}>
            <img src={article.cover} alt="" />
          </div>
          <div className={styles.detailBody}>
            <div>
              <div className={styles.headerText}>
                <span className={styles.kicker}>{article.level}</span>
                <h1>{article.title}</h1>
                <p>{article.summary}</p>
                <div className={styles.headerActions}>
                  {article.tags.map((tag) => (
                    <Tag key={tag}>{tag}</Tag>
                  ))}
                </div>
              </div>
              <p className={styles.articleText}>{article.content}</p>
            </div>
            <aside className={styles.metaBox}>
              <strong>{article.author.name}</strong>
              <p className={styles.helper}>{article.author.title}</p>
              <div className={styles.insightList}>
                <div className={styles.insight}>
                  <span>
                    <Clock size={18} />
                  </span>
                  <div>
                    <strong>{article.minutes} 分钟</strong>
                    <p>建议阅读时间</p>
                  </div>
                </div>
                <div className={styles.insight}>
                  <span>
                    <Eye size={18} />
                  </span>
                  <div>
                    <strong>{article.reads.toLocaleString()}</strong>
                    <p>累计阅读</p>
                  </div>
                </div>
              </div>
              <Button variant="secondary" icon={<Bookmark size={18} />}>
                收藏
              </Button>
              <Button variant="ghost" icon={<Heart size={18} />}>
                喜欢
              </Button>
            </aside>
          </div>
        </article>
      )}
    </AppLayout>
  );
};

export default PostDetailPage;
