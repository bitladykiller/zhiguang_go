import { useEffect, useState } from "react";
import AppLayout from "@/components/layout/AppLayout";
import MainHeader from "@/components/layout/MainHeader";
import CourseCard from "@/components/cards/CourseCard";
import LikeFavBar from "@/components/common/LikeFavBar";
import { knowpostService } from "@/services/knowpostService";
import AuthStatus from "@/features/auth/AuthStatus";
import styles from "./HomePage.module.css";

const skeletonCards = Array.from({ length: 8 }, (_, index) => index);

const HomePage = () => {
  const [items, setItems] = useState<Array<{
    id: string;
    title: string;
    description: string;
    coverImage?: string;
    tags: string[];
    tagJson?: string;
    authorAvatar?: string;
    authorAvator?: string;
    authorNickname: string;
    likeCount?: number;
    favoriteCount?: number;
    liked?: boolean;
    faved?: boolean;
  }>>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const run = async () => {
      setLoading(true);
      setError(null);
      try {
        const resp = await knowpostService.feed(1, 20);
        if (!cancelled) {
          setItems(resp.items ?? []);
        }
      } catch (err) {
        const msg = err instanceof Error ? err.message : "加载失败";
        if (!cancelled) setError(msg);
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    run();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <AppLayout
      header={
        <MainHeader
          headline="知光 · 让思想有温度，让知识会发光"
          subtitle="发现、收藏并沉淀值得长期学习的知文"
          rightSlot={<AuthStatus />}
        />
      }
      variant="cardless"
    >
      <section className={styles.feedToolbar} aria-label="内容概览">
        <div>
          <span className={styles.toolbarEyebrow}>今日推荐</span>
          <strong>{loading ? "正在刷新内容" : `已加载 ${items.length} 篇知文`}</strong>
        </div>
        <span className={styles.toolbarNote}>按发布时间与互动热度综合排序</span>
      </section>

      {error ? (
        <div className={styles.stateBanner} role="alert">
          <strong>内容加载失败</strong>
          <span>{error}</span>
        </div>
      ) : null}

      <div className={styles.masonry}>
        {loading && items.length === 0
          ? skeletonCards.map(item => (
              <div key={item} className={styles.masonryItem}>
                <div className={styles.skeletonCard}>
                  <div className={styles.skeletonCover} />
                  <div className={styles.skeletonLineStrong} />
                  <div className={styles.skeletonLine} />
                  <div className={styles.skeletonMeta} />
                </div>
              </div>
            ))
          : null}

        {items.map(item => (
          <div key={item.id} className={styles.masonryItem}>
            <CourseCard
              id={item.id}
              title={item.title}
              summary={item.description ?? ""}
              tags={item.tags ?? []}
              authorTags={(() => {
                try {
                  return item.tagJson ? (JSON.parse(item.tagJson) as unknown[]).filter((t) => typeof t === "string") as string[] : [];
                } catch {
                  return [];
                }
              })()}
              teacher={{ name: item.authorNickname, avatarUrl: item.authorAvatar ?? item.authorAvator }}
              coverImage={item.coverImage}
              to={`/post/${item.id}`}
              footerExtra={<LikeFavBar entityId={item.id} compact initialCounts={{ like: item.likeCount ?? 0, fav: item.favoriteCount ?? 0 }} initialState={{ liked: item.liked, faved: item.faved }} />}
            />
          </div>
        ))}
      </div>

      {!loading && items.length === 0 ? (
        <div className={styles.emptyState}>
          <div className={styles.emptyMark}>知</div>
          <strong>{error ? "暂时没有可展示内容" : "还没有公开知文"}</strong>
          <span>{error ? "请稍后刷新，或确认后端服务已启动。" : "发布第一篇知文后，这里会展示最新内容。"}</span>
        </div>
      ) : null}
    </AppLayout>
  );
};

export default HomePage;
