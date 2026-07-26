import clsx from "clsx";
import styles from "@/components/decor/Constellation.module.css";

/** 主星座的连线路径与星点（手绘的抽象星图，非真实天文数据）。 */
const MAJOR: Array<[number, number, number]> = [
  [12, 86, 2],
  [42, 60, 2.6],
  [74, 70, 2],
  [106, 48, 3],
  [134, 58, 2.2],
  [166, 30, 2.6],
  [194, 42, 2]
];

const MINOR: Array<[number, number]> = [
  [28, 22],
  [58, 100],
  [96, 14],
  [126, 96],
  [152, 78],
  [182, 12],
  [200, 78],
  [8, 40]
];

/** 装饰性星座图：金线连缀的星点，静静明灭。 */
const Constellation = ({ className }: { className?: string }) => {
  return (
    <svg
      className={clsx(styles.svg, className)}
      viewBox="0 0 208 112"
      fill="none"
      aria-hidden="true"
      focusable="false"
    >
      <polyline
        className={styles.thread}
        points={MAJOR.map(([x, y]) => `${x},${y}`).join(" ")}
      />
      {MAJOR.map(([x, y, r], index) => (
        <g key={`major-${index}`}>
          <circle className={styles.halo} cx={x} cy={y} r={r * 2.6} />
          <circle className={styles.star} cx={x} cy={y} r={r} />
        </g>
      ))}
      {MINOR.map(([x, y], index) => (
        <circle key={`minor-${index}`} className={styles.minor} cx={x} cy={y} r={1.1} />
      ))}
    </svg>
  );
};

export default Constellation;
