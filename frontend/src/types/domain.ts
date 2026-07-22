export type User = {
  id: string;
  name: string;
  title: string;
  email?: string;
  avatar?: string;
  skills: string[];
};

export type Article = {
  id: string;
  title: string;
  summary: string;
  content: string;
  cover: string;
  tags: string[];
  author: User;
  likes: number;
  favorites: number;
  reads: number;
  minutes: number;
  level: "入门" | "进阶" | "实战" | "体系";
  publishedAt: string;
  pinned?: boolean;
};

export type PublishDraft = {
  title: string;
  summary: string;
  content: string;
  tags: string[];
  visibility: "public" | "private";
};
