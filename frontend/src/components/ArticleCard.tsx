import { Bookmark, Clock, Eye, Heart, Pin } from "lucide-react";
import { Link } from "react-router-dom";
import Tag from "@/components/ui/Tag";
import type { Article } from "@/types/domain";
import styles from "@/components/ArticleCard.module.css";

type ArticleCardProps = {
  article: Article;
  featured?: boolean;
};

const ArticleCard = ({ article, featured = false }: ArticleCardProps) => {
  return (
    <article className={featured ? styles.featured : styles.card}>
      <Link to={`/post/${article.id}`} className={styles.mediaLink} aria-label={article.title}>
        <img src={article.cover} alt="" className={styles.cover} loading="lazy" />
        {article.pinned ? (
          <span className={styles.pin}>
            <Pin size={14} />
            置顶
          </span>
        ) : null}
      </Link>

      <div className={styles.body}>
        <div className={styles.tags}>
          <Tag tone="gold">{article.level}</Tag>
          {article.tags.slice(0, 2).map((tag) => (
            <Tag key={tag}>{tag}</Tag>
          ))}
        </div>
        <Link to={`/post/${article.id}`} className={styles.title}>
          {article.title}
        </Link>
        <p className={styles.summary}>{article.summary}</p>
      </div>

      <footer className={styles.footer}>
        <div className={styles.author}>
          <span className={styles.avatar}>{article.author.name.slice(0, 1)}</span>
          <div>
            <strong>{article.author.name}</strong>
            <small>{article.author.title}</small>
          </div>
        </div>
        <div className={styles.stats}>
          <span>
            <Clock size={15} />
            {article.minutes} 分钟
          </span>
          <span>
            <Eye size={15} />
            {article.reads.toLocaleString()}
          </span>
          <span>
            <Heart size={15} />
            {article.likes}
          </span>
          <span>
            <Bookmark size={15} />
            {article.favorites}
          </span>
        </div>
      </footer>
    </article>
  );
};

export default ArticleCard;
