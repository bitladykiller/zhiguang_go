import clsx from "clsx";
import styles from "@/components/decor/Ornament.module.css";

type OrnamentProps = {
  label?: string;
  align?: "center" | "start";
  className?: string;
};

/** 鎏金分隔线：两侧细线夹一枚菱星，可在中间放置章节小字。 */
const Ornament = ({ label, align = "center", className }: OrnamentProps) => {
  return (
    <div className={clsx(styles.ornament, align === "start" && styles.start, className)} aria-hidden="true">
      <span className={styles.line} />
      {label ? <em className={styles.label}>{label}</em> : <i className={styles.diamond} />}
      <span className={styles.line} />
    </div>
  );
};

export default Ornament;
