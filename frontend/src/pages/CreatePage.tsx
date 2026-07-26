import clsx from "clsx";
import { Feather, Send } from "lucide-react";
import { useState } from "react";
import Ornament from "@/components/decor/Ornament";
import Button from "@/components/ui/Button";
import AppLayout from "@/layouts/AppLayout";
import { contentService } from "@/services/contentService";
import type { PublishDraft } from "@/types/domain";
import styles from "@/pages/PageStyles.module.css";
import create from "@/pages/CreatePage.module.css";

const initialDraft: PublishDraft = {
  title: "",
  summary: "",
  content: "",
  tags: [],
  visibility: "public"
};

const GUIDE = [
  { title: "标题表达观点", desc: "一句话立场，比「关于 X 的一些思考」更值得点开。" },
  { title: "摘要说明价值", desc: "读者读完能带走什么？先在摘要里承诺清楚。" },
  { title: "正文沉淀方法", desc: "背景、问题、方法、验证、风险，五段自成闭环。" },
  { title: "标签便于检索", desc: "两到三个准确标签，胜过十个宽泛标签。" }
];

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
        <section className={clsx(styles.pageHead, "rise", "d1")}>
          <div className={styles.pageHeadText}>
            <span className={styles.kicker}>灯下 · WRITE</span>
            <h1>研好墨，写一篇值得收藏的知文。</h1>
            <p>标题表达观点，摘要说明价值，正文沉淀方法。页面先提供稳定工程化表单，后续可接图片上传和草稿 API。</p>
          </div>
        </section>

        <div className={create.layout}>
          <section className={clsx(styles.formPanel, "rise", "d2")}>
            <div className={styles.formGrid}>
              <div className={styles.field}>
                <label htmlFor="title">标题 · TITLE</label>
                <input
                  id="title"
                  className={styles.input}
                  value={draft.title}
                  placeholder="例如：如何设计一个可靠的 Agent 工作流"
                  onChange={(event) => setDraft((prev) => ({ ...prev, title: event.target.value }))}
                />
              </div>
              <div className={styles.field}>
                <label htmlFor="visibility">可见性 · SCOPE</label>
                <select
                  id="visibility"
                  className={styles.select}
                  value={draft.visibility}
                  onChange={(event) =>
                    setDraft((prev) => ({ ...prev, visibility: event.target.value as PublishDraft["visibility"] }))
                  }
                >
                  <option value="public">公开</option>
                  <option value="private">私密</option>
                </select>
              </div>
              <div className={styles.fullField}>
                <label htmlFor="summary">摘要 · SUMMARY</label>
                <input
                  id="summary"
                  className={styles.input}
                  value={draft.summary}
                  placeholder="用一句话说明读者能获得什么"
                  onChange={(event) => setDraft((prev) => ({ ...prev, summary: event.target.value }))}
                />
              </div>
              <div className={styles.fullField}>
                <label htmlFor="tags">标签 · TAGS</label>
                <input id="tags" className={styles.input} value={tagText} onChange={(event) => setTagText(event.target.value)} />
                <span className={styles.helper}>用英文逗号分隔，例如：RAG, Go, 工程化</span>
              </div>
              <div className={styles.fullField}>
                <label htmlFor="content">正文 · BODY</label>
                <span className={styles.count}>{draft.content.length} 字</span>
                <textarea
                  id="content"
                  className={styles.textarea}
                  value={draft.content}
                  placeholder="写下背景、问题、方法、验证和风险……"
                  onChange={(event) => setDraft((prev) => ({ ...prev, content: event.target.value }))}
                />
              </div>
            </div>
            {message ? <div className={styles.message}>{message}</div> : null}
            {error ? <div className={styles.error}>{error}</div> : null}
            <div className={create.submitRow}>
              <span className={create.submitHint}>灯下所写，皆可成光</span>
              <Button icon={<Send size={17} />} onClick={publish}>
                发布知文
              </Button>
            </div>
          </section>

          <aside className={clsx(create.guidePanel, "rise", "d3")}>
            <div className={create.guideHead}>
              <span>笔 意 · CRAFT</span>
              <strong>创作四则</strong>
            </div>
            <Ornament />
            {GUIDE.map((item, index) => (
              <div key={item.title} className={create.tip}>
                <span className={create.tipIndex}>{["壹", "贰", "叁", "肆"][index]}</span>
                <div>
                  <strong>{item.title}</strong>
                  <p>{item.desc}</p>
                </div>
              </div>
            ))}
            <Ornament />
            <div className={create.tip}>
              <span className={create.tipIndex}>
                <Feather size={14} />
              </span>
              <div>
                <strong>写不出来？</strong>
                <p>先写「我今天解决了什么问题」，方法自然会跟着流出来。</p>
              </div>
            </div>
          </aside>
        </div>
      </div>
    </AppLayout>
  );
};

export default CreatePage;
