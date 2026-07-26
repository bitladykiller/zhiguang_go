import clsx from "clsx";
import styles from "@/components/decor/SealMark.module.css";

type SealMarkProps = {
  text?: string;
  size?: "sm" | "md" | "lg";
  className?: string;
};

/** 朱砂印章：竖排双字的装饰性钤印，默认「知光」。 */
const SealMark = ({ text = "知光", size = "md", className }: SealMarkProps) => {
  return (
    <span className={clsx(styles.seal, styles[size], className)} aria-hidden="true">
      {text}
    </span>
  );
};

export default SealMark;
