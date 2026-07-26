import { ArrowRight, Bookmark, Clock, Eye, Heart, Pin } from "lucide-react";
import { Link } from "react-router-dom";
import SealMark from "@/components/decor/SealMark";
import Tag from "@/components/ui/Tag";
import type { Article } from "@/types/domain";
import styles from "@/components/ArticleCard.module.css";

type ArticleCardProps = {
  article: Article;
  featured?: boolean;
};

const levelTone = (level: Article["level"]) => {
  switch (level) {
    case "入门":
      return "jade" as const;
    case "进阶":
      return "blue" as const;
    case "体系":
      return "seal" as const;
    default:
      return "gold" as const;
  }
};

const Stats = ({ article }: { article: Article }) => (
  <div className={styles.stats}>
    <span>
      <Clock size={14} />
      {article.minutes} 分钟
    </span>
    <span>
      <Eye size={14} />
      {article.reads.toLocaleString()}
    </span>
    <span>
      <Heart size={14} />
      {article.likes}
    </span>
    <span>
      <Bookmark size={14} />
      {article.favorites}
    </span>
  </div>
);

const Author = ({ article }: { article: Article }) => (
  <div className={styles.author}>
    <span className={styles.avatar}>{article.author.name.slice(0, 1)}</span>
    <div>
      <strong>{article.author.name}</strong>
      <small>{article.author.title}</small>
    </div>
  </div>
);

const ArticleCard = ({ article, featured = false }: ArticleCardProps) => {
  if (featured) {
    return (
      <article className={styles.featured}>
        <i className={styles.cornerTL} aria-hidden="true" />
        <i className={styles.cornerBR} aria-hidden="true" />
        <span className={styles.featureRibbon} aria-hidden="true">
          封面故事 · FEATURE
        </span>
        <div className={styles.featuredBody}>
          <div className={styles.tags}>
            <Tag tone={levelTone(article.level)}>{article.level}</Tag>
            {article.tags.slice(0, 3).map((tag) => (
              <Tag key={tag}>{tag}</Tag>
            ))}
          </div>
          <Link to={`/post/${article.id}`} className={styles.featuredTitle}>
            {article.title}
          </Link>
          <p className={styles.featuredSummary}>{article.summary}</p>
          <div className={styles.featuredFooter}>
            <Author article={article} />
            <Stats article={article} />
          </div>
          <Link to={`/post/${article.id}`} className={styles.readCta}>
            展开夜读
            <ArrowRight size={16} />
          </Link>
        </div>
        <Link to={`/post/${article.id}`} className={styles.featuredMedia} aria-label={article.title} tabIndex={-1}>
          <img src={article.cover} alt="" loading="lazy" />
          <SealMark className={styles.mediaSeal} size="md" />
        </Link>
      </article>
    );
  }

  return (
    <article className={styles.card}>
      <Link to={`/post/${article.id}`} className={styles.mediaLink} aria-label={article.title}>
        <img src={article.cover} alt="" className={styles.cover} loading="lazy" />
        {article.pinned ? (
          <span className={styles.pin}>
            <Pin size={13} />
            置顶
          </span>
        ) : null}
      </Link>

      <div className={styles.body}>
        <div className={styles.tags}>
          <Tag tone={levelTone(article.level)}>{article.level}</Tag>
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
        <Author article={article} />
        <Stats article={article} />
      </footer>
    </article>
  );
};

export default ArticleCard;
