import { ArrowRight, Edit3, Layers3, Sparkles } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import MetricCard from "@/components/MetricCard";
import SearchBox from "@/components/SearchBox";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";

const HomePage = () => {
  const [articles, setArticles] = useState<Article[]>([]);
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    let cancelled = false;
    void contentService.feed().then((items) => {
      if (!cancelled) {
        setArticles(items);
        setLoading(false);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const featured = useMemo(() => articles.find((item) => item.pinned) ?? articles[0], [articles]);
  const regular = featured ? articles.filter((item) => item.id !== featured.id) : articles;

  const submitSearch = () => {
    if (query.trim()) navigate(`/search?q=${encodeURIComponent(query.trim())}`);
  };

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={styles.header}>
          <div className={styles.headerText}>
            <span className={styles.kicker}>精选知识工作台</span>
            <h1>把灵感、课程和工程经验沉淀成高质量知文。</h1>
            <p>知光面向学习者和创作者，提供搜索、发布、收藏、学习追踪和个人主页的一体化体验。</p>
            <SearchBox value={query} onChange={setQuery} onSubmit={submitSearch} />
          </div>
          <div className={styles.headerActions}>
            <Button variant="secondary" icon={<Edit3 size={18} />} onClick={() => navigate("/create")}>
              发布知文
            </Button>
            <Button variant="ghost" icon={<ArrowRight size={18} />} onClick={() => navigate("/search")}>
              探索主题
            </Button>
          </div>
        </section>

        <div className={styles.metricsGrid}>
          <MetricCard label="精选知文" value="128" caption="围绕 AI、后端、产品化学习持续更新" />
          <MetricCard label="平均阅读" value="8.6m" caption="适合通勤、碎片复盘和深度学习前预热" />
          <MetricCard label="创作者" value="42" caption="工程师、研究者和产品学习者共同沉淀" />
        </div>

        <div className={styles.workspace}>
          <section className={styles.section}>
            <div className={styles.sectionHead}>
              <div className={styles.sectionTitle}>
                <h2>今日主推</h2>
                <p>选择一篇先读透，再进入相关主题。</p>
              </div>
            </div>
            {loading ? (
              <EmptyState title="正在加载内容" description="正在连接知光内容服务。" />
            ) : featured ? (
              <ArticleCard article={featured} featured />
            ) : (
              <EmptyState title="暂无内容" description="发布第一篇知文后会显示在这里。" />
            )}
          </section>

          <aside className={styles.section}>
            <div className={styles.sectionHead}>
              <div className={styles.sectionTitle}>
                <h2>学习信号</h2>
                <p>今天值得关注的三个方向。</p>
              </div>
            </div>
            <div className={styles.insightList}>
              <div className={styles.insight}>
                <span>
                  <Sparkles size={18} />
                </span>
                <div>
                  <strong>Agent 工程</strong>
                  <p>从一次性调用升级到可观测、可恢复的任务工作流。</p>
                </div>
              </div>
              <div className={styles.insight}>
                <span>
                  <Layers3 size={18} />
                </span>
                <div>
                  <strong>后端可靠性</strong>
                  <p>缓存、队列、事务和可观测错误处理仍是核心能力。</p>
                </div>
              </div>
              <div className={styles.insight}>
                <span>知</span>
                <div>
                  <strong>知识资产</strong>
                  <p>把笔记拆成可以复用的概念、案例、问题和行动。</p>
                </div>
              </div>
            </div>
          </aside>
        </div>

        <section className={styles.section}>
          <div className={styles.sectionHead}>
            <div className={styles.sectionTitle}>
              <h2>最新知文</h2>
              <p>结构清晰、可收藏、能直接进入实践。</p>
            </div>
          </div>
          <div className={styles.grid}>
            {regular.map((article) => (
              <ArticleCard key={article.id} article={article} />
            ))}
          </div>
        </section>
      </div>
    </AppLayout>
  );
};

export default HomePage;
