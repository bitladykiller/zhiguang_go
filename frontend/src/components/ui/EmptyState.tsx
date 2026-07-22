import { FileText } from "lucide-react";
import Button from "@/components/ui/Button";
import styles from "@/components/ui/EmptyState.module.css";

type EmptyStateProps = {
  title: string;
  description: string;
  action?: {
    label: string;
    onClick: () => void;
  };
};

const EmptyState = ({ title, description, action }: EmptyStateProps) => {
  return (
    <div className={styles.empty}>
      <div className={styles.icon}>
        <FileText size={26} strokeWidth={1.8} />
      </div>
      <strong>{title}</strong>
      <p>{description}</p>
      {action ? (
        <Button variant="ghost" onClick={action.onClick}>
          {action.label}
        </Button>
      ) : null}
    </div>
  );
};

export default EmptyState;
