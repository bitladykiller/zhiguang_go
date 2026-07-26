import clsx from "clsx";
import { PlayCircle, Target } from "lucide-react";
import Constellation from "@/components/decor/Constellation";
import Ornament from "@/components/decor/Ornament";
import Tag from "@/components/ui/Tag";
import Button from "@/components/ui/Button";
import AppLayout from "@/layouts/AppLayout";
import styles from "@/pages/PageStyles.module.css";
import learn from "@/pages/LearningPage.module.css";

const steps = [
  { title: "建立知识系统", desc: "先理解输入、沉淀和复盘的闭环。", done: true },
  { title: "实践 Agent 工作流", desc: "把工具调用变成可恢复的流程。", done: true },
  { title: "补齐 Go 后端可靠性", desc: "缓存、队列和可观测错误处理。", done: false },
  { title: "完成一次公开输出", desc: "发布一篇可被收藏和搜索的知文。", done: false }
];

const LearningPage = () => {
  const doneCount = steps.filter((step) => step.done).length;
  const percent = Math.round((doneCount / steps.length) * 100);

  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={clsx(styles.pageHead, "rise", "d1")}>
          <div className={styles.pageHeadText}>
            <span className={styles.kicker}>星轨 · STUDY</span>
            <h1>把阅读，变成一条持续推进的星轨。</h1>
            <p>学习页聚焦路径、进度和下一步行动，后续可以接购买课程、学习记录和完成率接口。</p>
          </div>
          <div className={styles.headerActions}>
            <Button variant="primary" icon={<PlayCircle size={17} />}>
              继续学习
            </Button>
          </div>
          <Constellation className={styles.pageSky} />
        </section>

        <div className={learn.layout}>
          <section className={clsx(learn.pathPanel, "rise", "d2")}>
            <header className={styles.sectionHead}>
              <div className={styles.sectionTitle}>
                <span className={styles.chapter}>本周星轨 · PATH</span>
                <h2>从知识系统到工程输出</h2>
                <p>每完成一步，就点亮一颗星。</p>
              </div>
              <span className={styles.sectionRule} aria-hidden="true" />
            </header>
            <div className={learn.steps}>
              {steps.map((step, index) => (
                <div key={step.title} className={clsx(learn.step, step.done && learn.stepDone)}>
                  <span className={step.done ? learn.nodeDone : learn.nodeTodo}>{step.done ? "✦" : index + 1}</span>
                  <div className={learn.stepBody}>
                    <strong>{step.title}</strong>
                    <p>{step.desc}</p>
                  </div>
                  <span className={learn.stepState}>{step.done ? "已点亮" : "待点亮"}</span>
                </div>
              ))}
            </div>
          </section>

          <aside className={clsx(learn.rail, "rise", "d3")}>
            <div className={learn.progressPanel}>
              <span className={styles.chapter}>进度 · PROGRESS</span>
              <span className={learn.fraction}>
                {doneCount}
                <small> / {steps.length}</small>
              </span>
              <div className={learn.barTrack}>
                <div className={learn.barFill} style={{ width: `${percent}%` }} />
              </div>
              <p className={learn.progressCaption}>
                已点亮 {doneCount} 颗星，完成度 {percent}%。下一步：{steps.find((step) => !step.done)?.title ?? "全部完成"}。
              </p>
            </div>

            <div className={learn.focusPanel}>
              <Tag tone="jade">本周聚焦</Tag>
              <strong>后端可靠性专题</strong>
              <p>缓存穿透、消息幂等与可观测错误处理，是把 Demo 变成生产系统的三道门。</p>
              <Ornament />
              <div className={styles.insight}>
                <span>
                  <Target size={17} />
                </span>
                <div>
                  <strong>本周目标</strong>
                  <p>完成「Go 后端可靠性」并输出一篇实践笔记。</p>
                </div>
              </div>
            </div>
          </aside>
        </div>
      </div>
    </AppLayout>
  );
};

export default LearningPage;
