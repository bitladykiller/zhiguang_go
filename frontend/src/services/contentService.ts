import { requestJson } from "@/services/apiClient";
import type { Article, PublishDraft, User } from "@/types/domain";

const authors: User[] = [
  {
    id: "u-1",
    name: "林知远",
    title: "AI 应用架构师",
    skills: ["RAG", "Agent", "评估"]
  },
  {
    id: "u-2",
    name: "周明澈",
    title: "Go 后端工程师",
    skills: ["Go", "Kafka", "Redis"]
  },
  {
    id: "u-3",
    name: "沈一宁",
    title: "产品化学习研究员",
    skills: ["学习系统", "知识管理"]
  }
];

export const mockArticles: Article[] = [
  {
    id: "systems-thinking",
    title: "把知识库做成系统，而不是文件夹",
    summary: "从信息采集、结构化、复盘到检索，搭建一个可以长期复利的个人知识系统。",
    content: "真正可靠的知识系统不是资料越多越好，而是每条信息都能被复用、被追踪、被验证。建议先定义输入标准，再把笔记拆成概念、案例、问题和行动四类，最后通过周期复盘持续压缩噪声。",
    cover: "/covers/systems-thinking.png",
    tags: ["知识管理", "系统设计", "复盘"],
    author: authors[2],
    likes: 842,
    favorites: 386,
    reads: 12800,
    minutes: 8,
    level: "体系",
    publishedAt: "2026-07-20",
    pinned: true
  },
  {
    id: "agent-workflow",
    title: "从工具调用到稳定 Agent 工作流",
    summary: "拆解 Planner、Executor、Reviewer 的职责边界，让 Agent 不再只会一次性回答。",
    content: "工程化 Agent 的核心是显式状态、工具边界和终止条件。Planner 负责拆任务，Executor 只执行可验证动作，Reviewer 对结果做约束检查。每一层都要有超时、重试和日志。",
    cover: "/covers/agent-workflow.png",
    tags: ["Agent", "工具调用", "工作流"],
    author: authors[0],
    likes: 629,
    favorites: 274,
    reads: 9400,
    minutes: 11,
    level: "实战",
    publishedAt: "2026-07-18"
  },
  {
    id: "go-backend",
    title: "Go Web 后端里的可观测错误处理",
    summary: "用错误分层、Trace ID 和结构化日志，减少线上排障的不确定性。",
    content: "Handler 层负责把业务错误转换为 HTTP 状态码，Service 层表达业务规则，Repository 层保留原始数据库错误。日志只记录必要上下文，不能泄露 Token、密码或私钥。",
    cover: "/covers/go-backend.png",
    tags: ["Go", "后端", "可观测性"],
    author: authors[1],
    likes: 512,
    favorites: 198,
    reads: 7600,
    minutes: 9,
    level: "进阶",
    publishedAt: "2026-07-16"
  },
  {
    id: "rag-notes",
    title: "RAG 评估不要只看答案像不像",
    summary: "把召回、引用、答案稳定性和安全边界拆开评估，才能真正定位问题。",
    content: "RAG：Retrieval-Augmented Generation，检索增强生成。评估时至少拆成检索召回、上下文相关性、答案忠实度、拒答能力和延迟成本五类指标。",
    cover: "/covers/rag-notes.png",
    tags: ["RAG", "评估", "LLM"],
    author: authors[0],
    likes: 474,
    favorites: 229,
    reads: 6800,
    minutes: 7,
    level: "进阶",
    publishedAt: "2026-07-14"
  },
  {
    id: "product-study",
    title: "把学习路径设计成产品漏斗",
    summary: "目标、反馈、激励和复盘缺一不可，学习产品也需要转化漏斗。",
    content: "学习路径不是课程列表。一个好的路径应该明确当前阶段、下一步动作、反馈周期和可量化成果，让用户知道自己为什么继续学。",
    cover: "/covers/product-study.png",
    tags: ["学习路径", "产品", "增长"],
    author: authors[2],
    likes: 398,
    favorites: 156,
    reads: 5900,
    minutes: 6,
    level: "入门",
    publishedAt: "2026-07-12"
  },
  {
    id: "design-lab",
    title: "工程化前端不等于没有审美",
    summary: "设计 token、组件边界和视觉系统可以同时服务工程效率与视觉品质。",
    content: "前端工程化不是只关心目录结构。设计 token 让颜色、间距、圆角、阴影和状态可复用；组件边界让页面可以快速组合；视觉系统让产品体验稳定。",
    cover: "/covers/design-lab.png",
    tags: ["前端", "设计系统", "工程化"],
    author: authors[1],
    likes: 731,
    favorites: 342,
    reads: 11000,
    minutes: 10,
    level: "体系",
    publishedAt: "2026-07-10"
  }
];

type FeedResponse = {
  items?: unknown[];
};

const normalizeArticle = (item: any): Article => ({
  id: String(item.id ?? crypto.randomUUID()),
  title: String(item.title ?? "未命名知文"),
  summary: String(item.description ?? item.summary ?? ""),
  content: String(item.content ?? item.description ?? ""),
  cover: String(item.coverImage ?? item.cover ?? "/covers/design-lab.png"),
  tags: Array.isArray(item.tags) ? item.tags.map(String) : [],
  author: {
    id: String(item.authorId ?? "remote"),
    name: String(item.authorNickname ?? item.author?.name ?? "知光作者"),
    title: String(item.authorTitle ?? "创作者"),
    avatar: item.authorAvatar,
    skills: []
  },
  likes: Number(item.likeCount ?? item.likes ?? 0),
  favorites: Number(item.favoriteCount ?? item.favorites ?? 0),
  reads: Number(item.reads ?? item.viewCount ?? 0),
  minutes: Number(item.minutes ?? 6),
  level: item.level ?? "实战",
  publishedAt: String(item.publishedAt ?? item.createdAt ?? ""),
  pinned: Boolean(item.isTop ?? item.pinned)
});

export const contentService = {
  async feed(): Promise<Article[]> {
    try {
      const resp = await requestJson<FeedResponse>("/api/v1/knowposts/feed?page=1&size=20");
      return (resp.items ?? []).map(normalizeArticle);
    } catch {
      return mockArticles;
    }
  },

  async search(keyword: string): Promise<Article[]> {
    const q = keyword.trim().toLowerCase();
    if (!q) return [];
    try {
      const resp = await requestJson<FeedResponse>(`/api/v1/search?q=${encodeURIComponent(q)}&size=20`);
      return (resp.items ?? []).map(normalizeArticle);
    } catch {
      return mockArticles.filter((item) =>
        [item.title, item.summary, item.tags.join(" ")].join(" ").toLowerCase().includes(q)
      );
    }
  },

  async detail(id: string): Promise<Article | null> {
    try {
      const resp = await requestJson<any>(`/api/v1/knowposts/${id}`);
      return normalizeArticle(resp);
    } catch {
      return mockArticles.find((item) => item.id === id) ?? null;
    }
  },

  async publish(draft: PublishDraft): Promise<Article> {
    return {
      id: `local-${Date.now()}`,
      title: draft.title,
      summary: draft.summary,
      content: draft.content,
      cover: "/covers/design-lab.png",
      tags: draft.tags,
      author: authors[0],
      likes: 0,
      favorites: 0,
      reads: 0,
      minutes: Math.max(3, Math.ceil(draft.content.length / 320)),
      level: "实战",
      publishedAt: new Date().toISOString().slice(0, 10)
    };
  }
};
