import clsx from "clsx";
import { ArrowRight, Edit3, Layers3, Sparkles } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import Constellation from "@/components/decor/Constellation";
import SealMark from "@/components/decor/SealMark";
import MetricCard from "@/components/MetricCard";
import SearchBox from "@/components/SearchBox";
import Button from "@/components/ui/Button";
import EmptyState from "@/components/ui/EmptyState";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";
import home from "@/pages/HomePage.module.css";

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
        <section className={clsx(home.hero, "bloom")}>
          <i className={home.orbGold} aria-hidden="true" />
          <i className={home.orbBlue} aria-hidden="true" />
          <Constellation className={home.heroSky} />
          <SealMark size="lg" className={home.heroSeal} />
          <div className={home.heroInner}>
            <span className={clsx(styles.kicker, "rise", "d1")}>卷首 · 今夜开卷</span>
            <h1 className={clsx(home.heroTitle, "rise", "d2")}>
              长夜之中，
              <em className="gilded">知识自有光</em>。
            </h1>
            <p className={clsx(home.heroLede, "rise", "d3")}>
              知光是面向学习者与创作者的星夜书院——把灵感、课程与工程经验，
              沉淀成可检索、可收藏、可复用的知文。
            </p>
            <div className={clsx(home.heroSearch, "rise", "d4")}>
              <SearchBox value={query} onChange={setQuery} onSubmit={submitSearch} />
            </div>
            <div className={clsx(styles.headerActions, "rise", "d5")}>
              <Button variant="primary" icon={<Edit3 size={17} />} onClick={() => navigate("/create")}>
                发布知文
              </Button>
              <Button variant="secondary" icon={<ArrowRight size={17} />} onClick={() => navigate("/search")}>
                探索主题
              </Button>
            </div>
            <span className={clsx(home.heroFoot, "rise", "d6")}>SINCE MMXXVI · 以夜为幕，以知为光</span>
          </div>
        </section>

        <div className={clsx(home.metricsGrid, "rise", "d5")}>
          <MetricCard label="精选知文 / POSTS" value="128" caption="围绕 AI、后端、产品化学习持续更新" />
          <MetricCard label="平均阅读 / MINUTES" value="8.6" caption="适合通勤、碎片复盘和深度学习前预热" />
          <MetricCard label="创作者 / AUTHORS" value="42" caption="工程师、研究者和产品学习者共同沉淀" />
        </div>

        <div className={home.workspace}>
          <section className={clsx("rise", "d6")}>
            <header className={styles.sectionHead}>
              <div className={styles.sectionTitle}>
                <span className={styles.chapter}>卷一 · VOL.I</span>
                <h2>今日主推</h2>
                <p>选择一篇先读透，再进入相关主题。</p>
              </div>
              <span className={styles.sectionRule} aria-hidden="true" />
            </header>
            {loading ? (
              <EmptyState busy title="正在点灯" description="正在连接知光内容服务，摘取今夜的星光。" />
            ) : featured ? (
              <ArticleCard article={featured} featured />
            ) : (
              <EmptyState title="暂无内容" description="发布第一篇知文后，它会成为今晚的封面故事。" />
            )}
          </section>

          <aside className={clsx(home.signalsPanel, "rise", "d7")}>
            <header className={styles.sectionHead} style={{ marginBottom: 0 }}>
              <div className={styles.sectionTitle}>
                <span className={styles.chapter}>星象 · SIGNALS</span>
                <h2>学习信号</h2>
                <p>今夜值得关注的三个方向。</p>
              </div>
            </header>
            <div className={styles.insightList}>
              <div className={styles.insight}>
                <span>
                  <Sparkles size={17} />
                </span>
                <div>
                  <strong>Agent 工程</strong>
                  <p>从一次性调用升级到可观测、可恢复的任务工作流。</p>
                </div>
              </div>
              <div className={styles.insight}>
                <span>
                  <Layers3 size={17} />
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

        <section className={clsx("rise", "d8")}>
          <header className={styles.sectionHead}>
            <div className={styles.sectionTitle}>
              <span className={styles.chapter}>卷二 · VOL.II</span>
              <h2>最新知文</h2>
              <p>结构清晰、可收藏、能直接进入实践。</p>
            </div>
            <span className={styles.sectionRule} aria-hidden="true" />
          </header>
          {loading ? (
            <EmptyState busy title="正在加载" description="星光稍候即至。" />
          ) : (
            <div className={styles.grid}>
              {regular.map((article) => (
                <ArticleCard key={article.id} article={article} />
              ))}
            </div>
          )}
        </section>
      </div>
    </AppLayout>
  );
};

export default HomePage;
