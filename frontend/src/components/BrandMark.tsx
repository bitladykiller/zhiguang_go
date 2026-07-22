import styles from "@/components/BrandMark.module.css";

const BrandMark = ({ compact = false }: { compact?: boolean }) => {
  return (
    <div className={styles.brand} aria-label="知光">
      <div className={styles.mark}>知</div>
      {!compact ? (
        <div className={styles.text}>
          <strong>知光</strong>
          <span>ZhiGuang</span>
        </div>
      ) : null}
    </div>
  );
};

export default BrandMark;
