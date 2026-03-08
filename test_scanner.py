from fpdf import FPDF
import os

def create_test_pdf():
    # 确保目录存在
    if not os.path.exists('output'):
        os.makedirs('output')
    
    # 计算逻辑
    result = 3 + 45
    content = f"Test Result: 3 + 45 = {result}"
    
    # 生成 PDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt="Stock Scanner Test Report", ln=1, align='C')
    pdf.cell(200, 10, txt=content, ln=2, align='L')
    
    file_path = "output/test_result.pdf"
    pdf.output(file_path)
    print(f"✅ 成功生成测试文件: {file_path}")

if __name__ == "__main__":
    create_test_pdf()
