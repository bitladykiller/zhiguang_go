import { FileText, Loader2 } from "lucide-react";
import Button from "@/components/ui/Button";
import styles from "@/components/ui/EmptyState.module.css";

type EmptyStateProps = {
  title: string;
  description: string;
  busy?: boolean;
  action?: {
    label: string;
    onClick: () => void;
  };
};

const EmptyState = ({ title, description, busy = false, action }: EmptyStateProps) => {
  return (
    <div className={styles.empty}>
      <i className={styles.cornerTL} aria-hidden="true" />
      <i className={styles.cornerBR} aria-hidden="true" />
      <div className={styles.icon}>
        {busy ? <Loader2 className={styles.spin} size={26} strokeWidth={1.8} /> : <FileText size={26} strokeWidth={1.8} />}
      </div>
      <strong>{title}</strong>
      <p>{description}</p>
      {action ? (
        <Button variant="secondary" onClick={action.onClick}>
          {action.label}
        </Button>
      ) : null}
    </div>
  );
};

export default EmptyState;
