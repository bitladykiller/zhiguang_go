import styles from "@/components/MetricCard.module.css";

const MetricCard = ({ label, value, caption }: { label: string; value: string; caption: string }) => {
  return (
    <div className={styles.metric}>
      <span className={styles.label}>{label}</span>
      <strong className={styles.value}>{value}</strong>
      <small className={styles.caption}>{caption}</small>
      <i className={styles.rule} aria-hidden="true" />
    </div>
  );
};

export default MetricCard;
