import { CheckCircle2, Circle, PlayCircle } from "lucide-react";
import Button from "@/components/ui/Button";
import AppLayout from "@/layouts/AppLayout";
import styles from "@/pages/PageStyles.module.css";

const steps = [
  { title: "建立知识系统", desc: "先理解输入、沉淀和复盘的闭环。", done: true },
  { title: "实践 Agent 工作流", desc: "把工具调用变成可恢复的流程。", done: true },
  { title: "补齐 Go 后端可靠性", desc: "缓存、队列和可观测错误处理。", done: false },
  { title: "完成一次公开输出", desc: "发布一篇可被收藏和搜索的知文。", done: false }
];

const LearningPage = () => {
  return (
    <AppLayout>
      <div className={styles.page}>
        <section className={styles.header}>
          <div className={styles.headerText}>
            <span className={styles.kicker}>学习路径</span>
            <h1>把阅读变成持续推进的学习项目。</h1>
            <p>学习页先聚焦路径、进度和下一步行动，后续可以接购买课程、学习记录和完成率接口。</p>
          </div>
          <div className={styles.headerActions}>
            <Button variant="secondary" icon={<PlayCircle size={18} />}>
              继续学习
            </Button>
          </div>
        </section>

        <section className={styles.section}>
          <div className={styles.sectionHead}>
            <div className={styles.sectionTitle}>
              <h2>本周路径</h2>
              <p>从知识系统到工程输出，逐步闭环。</p>
            </div>
          </div>
          <div className={styles.insightList}>
            {steps.map((step, index) => (
              <div key={step.title} className={styles.insight}>
                <span>{step.done ? <CheckCircle2 size={18} /> : <Circle size={18} />}</span>
                <div>
                  <strong>
                    {index + 1}. {step.title}
                  </strong>
                  <p>{step.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </AppLayout>
  );
};

export default LearningPage;
