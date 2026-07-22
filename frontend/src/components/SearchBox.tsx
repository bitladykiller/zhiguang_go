import { Search } from "lucide-react";
import styles from "@/components/SearchBox.module.css";

type SearchBoxProps = {
  value: string;
  placeholder?: string;
  onChange: (value: string) => void;
  onSubmit?: () => void;
};

const SearchBox = ({ value, placeholder = "搜索知识、作者或主题", onChange, onSubmit }: SearchBoxProps) => {
  return (
    <form
      className={styles.box}
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit?.();
      }}
    >
      <Search size={20} strokeWidth={1.8} />
      <input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} />
      <button type="submit">搜索</button>
    </form>
  );
};

export default SearchBox;
