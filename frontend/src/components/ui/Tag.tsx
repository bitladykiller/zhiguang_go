import clsx from "clsx";
import styles from "@/components/ui/Tag.module.css";

const Tag = ({ children, tone = "default" }: { children: React.ReactNode; tone?: "default" | "gold" | "blue" }) => {
  return <span className={clsx(styles.tag, styles[tone])}>{children}</span>;
};

export default Tag;
