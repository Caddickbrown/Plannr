import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import os

# GUI to load file and run material check
def load_file():
    filepath = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsm *.xlsx")])
    if not filepath:
        return

    try:
        df_main = pd.read_excel(filepath, sheet_name="Main")
        df_stock = pd.read_excel(filepath, sheet_name="StockTally")
        df_struct = pd.read_excel(filepath, sheet_name="ManStructures")

        # Preprocess data
        stock = df_stock.set_index("PART_NO")["Remaining"].to_dict()
        struct = df_struct[df_struct["Component Part"].notna()].copy()

        results = []

        for _, row in df_main.iterrows():
            so = row["SO Number"]
            part = row["Part"]
            qty = row["Demand"]

            bom = struct[struct["Parent Part"] == part]
            releasable = True
            shortage_list = []

            for _, comp in bom.iterrows():
                comp_part = comp["Component Part"]
                qpa = comp["QpA"]
                required_qty = qpa * qty
                available = stock.get(comp_part, 0)

                if available >= required_qty:
                    stock[comp_part] -= required_qty
                else:
                    releasable = False
                    shortage_list.append(f"{comp_part} (short {required_qty - available:.0f})")

            results.append({
                "Shop Order": so,
                "Part": part,
                "Qty": qty,
                "Releasable": "✅ Yes" if releasable else "❌ No",
                "Shortage": ", ".join(shortage_list) if shortage_list else "-"
            })

        # Save output
        output_file = os.path.join(os.path.dirname(filepath), "releasable_orders.csv")
        pd.DataFrame(results).to_csv(output_file, index=False)
        messagebox.showinfo("Success", f"Results saved to {output_file}")

    except Exception as e:
        messagebox.showerror("Error", str(e))

# Main app window
root = tk.Tk()
root.title("Releasable Order Material Checker")
root.geometry("400x200")

frame = tk.Frame(root)
frame.pack(expand=True)

tk.Label(frame, text="Click below to select your Excel file").pack(pady=20)
tk.Button(frame, text="Select File", command=load_file).pack()

root.mainloop()
