import { Send } from "lucide-react";
import { useState } from "react";
import Button from "@/components/ui/Button";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { PublishDraft } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";

const initialDraft: PublishDraft = {
  title: "",
  summary: "",
  content: "",
  tags: [],
  visibility: "public"
};

const CreatePage = () => {
  const [draft, setDraft] = useState<PublishDraft>(initialDraft);
  const [tagText, setTagText] = useState("AI, 工程实践");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const publish = async () => {
    setError("");
    setMessage("");
    if (!draft.title.trim()) {
      setError("请填写标题。");
      return;
    }
    if (!draft.content.trim()) {
      setError("请填写正文。");
      return;
    }
    const payload = {
      ...draft,
      tags: tagText
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    };
    await contentService.publish(payload);
    setMessage("知文已保存为本地草稿展示。接入后端后可替换为真实发布接口。");
  };

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={styles.header}>
          <div className={styles.headerText}>
            <span className={styles.kicker}>创作中心</span>
            <h1>写一篇结构清楚、值得收藏的知文。</h1>
            <p>标题表达观点，摘要说明价值，正文沉淀方法。页面先提供稳定工程化表单，后续可接图片上传和草稿 API。</p>
          </div>
        </section>

        <section className={styles.formPanel}>
          <div className={styles.formGrid}>
            <div className={styles.field}>
              <label htmlFor="title">标题</label>
              <input
                id="title"
                className={styles.input}
                value={draft.title}
                placeholder="例如：如何设计一个可靠的 Agent 工作流"
                onChange={(event) => setDraft((prev) => ({ ...prev, title: event.target.value }))}
              />
            </div>
            <div className={styles.field}>
              <label htmlFor="visibility">可见性</label>
              <select
                id="visibility"
                className={styles.select}
                value={draft.visibility}
                onChange={(event) => setDraft((prev) => ({ ...prev, visibility: event.target.value as PublishDraft["visibility"] }))}
              >
                <option value="public">公开</option>
                <option value="private">私密</option>
              </select>
            </div>
            <div className={styles.fullField}>
              <label htmlFor="summary">摘要</label>
              <input
                id="summary"
                className={styles.input}
                value={draft.summary}
                placeholder="用一句话说明读者能获得什么"
                onChange={(event) => setDraft((prev) => ({ ...prev, summary: event.target.value }))}
              />
            </div>
            <div className={styles.fullField}>
              <label htmlFor="tags">标签</label>
              <input id="tags" className={styles.input} value={tagText} onChange={(event) => setTagText(event.target.value)} />
              <span className={styles.helper}>用英文逗号分隔，例如：RAG, Go, 工程化</span>
            </div>
            <div className={styles.fullField}>
              <label htmlFor="content">正文</label>
              <textarea
                id="content"
                className={styles.textarea}
                value={draft.content}
                placeholder="写下背景、问题、方法、验证和风险..."
                onChange={(event) => setDraft((prev) => ({ ...prev, content: event.target.value }))}
              />
            </div>
          </div>
          {message ? <div className={styles.message}>{message}</div> : null}
          {error ? <div className={styles.error}>{error}</div> : null}
          <Button icon={<Send size={18} />} onClick={publish}>
            发布知文
          </Button>
        </section>
      </div>
    </AppLayout>
  );
};

export default CreatePage;
