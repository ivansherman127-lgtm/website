import "./datepicker.css";
import React from "react";
import { createRoot } from "react-dom/client";
import { DropdownRangeDatePicker } from "@/components/ui/dropdown-range-date-picker";

function App() {
  return (
    <div className="flex min-h-screen items-center justify-center">
      <DropdownRangeDatePicker />
    </div>
  );
}

const container = document.getElementById("root");
if (!container) throw new Error("#root not found");
createRoot(container).render(<App />);
