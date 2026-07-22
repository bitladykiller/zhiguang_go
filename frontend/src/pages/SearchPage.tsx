import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import ArticleCard from "@/components/ArticleCard";
import SearchBox from "@/components/SearchBox";
import EmptyState from "@/components/ui/EmptyState";
import AppLayout from "@/layouts/AppLayout";
import { contentService, mockArticles } from "@/services/contentService";
import type { Article } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";

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

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={styles.header}>
          <div className={styles.headerText}>
            <span className={styles.kicker}>全站搜索</span>
            <h1>用关键词找到可执行的知识。</h1>
            <p>搜索标题、摘要和标签。后端搜索服务不可用时，会自动用本地样例数据展示页面效果。</p>
            <SearchBox value={query} onChange={setQuery} onSubmit={submit} />
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.sectionHead}>
            <div className={styles.sectionTitle}>
              <h2>{params.get("q") ? `搜索结果：${params.get("q")}` : "热门主题"}</h2>
              <p>{loading ? "正在检索..." : params.get("q") ? `找到 ${results.length} 条内容` : "先从这些方向开始。"}</p>
            </div>
          </div>
          {params.get("q") && results.length === 0 && !loading ? (
            <EmptyState title="没有匹配结果" description="换一个关键词，或先浏览热门主题。" />
          ) : (
            <div className={styles.grid}>
              {(params.get("q") ? results : mockArticles).map((article) => (
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
