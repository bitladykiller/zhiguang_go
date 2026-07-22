import clsx from "clsx";
import type { ButtonHTMLAttributes, ReactNode } from "react";
import styles from "@/components/ui/Button.module.css";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost" | "danger";
  icon?: ReactNode;
};

const Button = ({ variant = "primary", icon, children, className, ...props }: ButtonProps) => {
  return (
    <button className={clsx(styles.button, styles[variant], className)} {...props}>
      {icon ? <span className={styles.icon}>{icon}</span> : null}
      <span>{children}</span>
    </button>
  );
};

export default Button;
