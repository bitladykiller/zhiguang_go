import clsx from "clsx";
import styles from "@/components/ui/Tag.module.css";

type TagTone = "default" | "gold" | "blue" | "seal" | "jade";

const Tag = ({ children, tone = "default" }: { children: React.ReactNode; tone?: TagTone }) => {
  return <span className={clsx(styles.tag, styles[tone])}>{children}</span>;
};

export default Tag;
