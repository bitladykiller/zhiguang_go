import clsx from "clsx";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import Constellation from "@/components/decor/Constellation";
import SearchBox from "@/components/SearchBox";
import EmptyState from "@/components/ui/EmptyState";
import AppLayout from "@/layouts/AppLayout";
import { contentService, mockArticles } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";
import search from "@/pages/SearchPage.module.css";

const HOT_TOPICS = ["RAG", "Agent", "Go", "知识管理", "设计系统", "复盘"];

const SearchPage = () => {
  const [params, setParams] = useSearchParams();
  const [query, setQuery] = useState(params.get("q") ?? "");
  const [results, setResults] = useState<Article[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const q = params.get("q") ?? "";
    setQuery(q);
    if (!q.trim()) {
      setResults([]);
      return;
    }
    setLoading(true);
    void contentService.search(q).then((items) => {
      setResults(items);
      setLoading(false);
    });
  }, [params]);

  const submit = () => {
    if (query.trim()) setParams({ q: query.trim() });
  };

  const activeQuery = params.get("q");

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={clsx(styles.pageHead, "rise", "d1")}>
          <div className={styles.pageHeadText}>
            <span className={styles.kicker}>寻星 · SEEK</span>
            <h1>以关键词为罗盘，找到可执行的知识。</h1>
            <p>搜索标题、摘要和标签。后端搜索服务不可用时，会自动用本地样例数据展示页面效果。</p>
            <SearchBox value={query} onChange={setQuery} onSubmit={submit} />
            <div className={search.topics}>
              <span className={search.topicsLabel}>热门星域</span>
              {HOT_TOPICS.map((topic) => (
                <button key={topic} type="button" className={search.topicChip} onClick={() => setParams({ q: topic })}>
                  {topic}
                </button>
              ))}
            </div>
          </div>
          <Constellation className={styles.pageSky} />
        </section>

        <section className={clsx("rise", "d3")}>
          <header className={styles.sectionHead}>
            <div className={styles.sectionTitle}>
              <span className={styles.chapter}>{activeQuery ? "检索 · RESULTS" : "巡览 · EXPLORE"}</span>
              <h2>{activeQuery ? `搜索结果：${activeQuery}` : "热门主题"}</h2>
              <p className={search.resultMeta}>
                {loading ? (
                  "正在检索星图…"
                ) : activeQuery ? (
                  <>
                    命中 <em>{results.length}</em> 条内容
                  </>
                ) : (
                  "先从这些方向开始。"
                )}
              </p>
            </div>
            <span className={styles.sectionRule} aria-hidden="true" />
          </header>
          {loading ? (
            <EmptyState busy title="正在检索" description="沿着星轨逐条比对标题、摘要与标签。" />
          ) : activeQuery && results.length === 0 ? (
            <EmptyState title="没有匹配结果" description="换一个关键词，或先浏览热门主题。" />
          ) : (
            <div className={styles.grid}>
              {(activeQuery ? results : mockArticles).map((article) => (
                <ArticleCard key={article.id} article={article} />
              ))}
            </div>
          )}
        </section>
      </div>
    </AppLayout>
  );
};

export default SearchPage;
