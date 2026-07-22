import styles from "@/components/MetricCard.module.css";

const MetricCard = ({ label, value, caption }: { label: string; value: string; caption: string }) => {
  return (
    <div className={styles.metric}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{caption}</small>
    </div>
  );
};

export default MetricCard;
