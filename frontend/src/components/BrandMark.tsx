import styles from "@/components/BrandMark.module.css";

const BrandMark = ({ compact = false }: { compact?: boolean }) => {
  return (
    <div className={styles.brand} aria-label="知光">
      <span className={styles.mark}>
        知
        <i className={styles.spark} aria-hidden="true" />
      </span>
      {!compact ? (
        <span className={styles.text}>
          <strong>知光</strong>
          <small>ZHIGUANG · 星夜书院</small>
        </span>
      ) : null}
    </div>
  );
};

export default BrandMark;
