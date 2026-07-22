import { Navigate, Route, Routes } from "react-router-dom";
import CreatePage from "@/pages/CreatePage";
import HomePage from "@/pages/HomePage";
import LearningPage from "@/pages/LearningPage";
import LoginPage from "@/pages/LoginPage";
import PostDetailPage from "@/pages/PostDetailPage";
import ProfilePage from "@/pages/ProfilePage";
import RegisterPage from "@/pages/RegisterPage";
import SearchPage from "@/pages/SearchPage";

const App = () => {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/search" element={<SearchPage />} />
      <Route path="/create" element={<CreatePage />} />
      <Route path="/learn" element={<LearningPage />} />
      <Route path="/profile" element={<ProfilePage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route path="/post/:id" element={<PostDetailPage />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
};

export default App;
