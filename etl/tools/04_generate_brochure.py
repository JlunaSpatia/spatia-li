from fpdf import FPDF
from PIL import Image, ImageDraw, ImageFont
import os
import random
import math

# --- CONFIGURACIÓN DE DISEÑO ---
COLOR_BG = (18, 18, 18)       # Negro Suave (Dark Mode)
COLOR_ACCENT = (243, 198, 35) # Amarillo "Blue Banana"
COLOR_TEXT = (240, 240, 240)  # Blanco Hueso
COLOR_DIM = (150, 150, 150)   # Gris para subtítulos

OUTPUT_DIR = "data/marketing"
if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

# --- 1. GENERADOR DE ARTE (LA PORTADA) ---
def create_hex_art(filename):
    """Genera una imagen abstracta de hexágonos (El 'Genoma')"""
    width, height = 1200, 800
    im = Image.new('RGB', (width, height), COLOR_BG)
    draw = ImageDraw.Draw(im)
    
    def draw_hexagon(x, y, size, color, fill=None):
        angle = 60
        coords = []
        for i in range(6):
            theta = math.radians(angle * i)
            px = x + size * math.cos(theta)
            py = y + size * math.sin(theta)
            coords.append((px, py))
        draw.polygon(coords, outline=color, fill=fill, width=3)

    # Dibujar Red Neuronal de Hexágonos
    center_x, center_y = width // 2, height // 2
    
    # Malla de fondo tenue
    for i in range(0, width, 60):
        for j in range(0, height, 52):
            offset = 30 if (j // 52) % 2 == 1 else 0
            draw_hexagon(i + offset, j, 28, (30, 30, 30))

    # Cluster "Ganador" (El Hotspot)
    for _ in range(15):
        off_x = random.randint(-100, 100)
        off_y = random.randint(-80, 80)
        # Hexágono Amarillo (Top Pick)
        draw_hexagon(center_x + off_x, center_y + off_y, 28, COLOR_ACCENT, fill=(COLOR_ACCENT[0], COLOR_ACCENT[1], COLOR_ACCENT[2], 100))

    # Guardar
    im.save(filename)
    print(f"   🎨 Arte generado: {filename}")

# --- 2. EL MAQUETADOR PDF ---
class PDF(FPDF):
    def header(self):
        # Fondo negro en todas las páginas
        self.set_fill_color(*COLOR_BG)
        self.rect(0, 0, 210, 297, 'F')
        # Logo (Texto)
        self.set_font('Arial', 'B', 10)
        self.set_text_color(*COLOR_DIM)
        self.cell(0, 10, 'SPATIA CONSULTING | Location Intelligence', 0, 1, 'R')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, title, subtitle=""):
        self.set_font('Arial', 'B', 24)
        self.set_text_color(*COLOR_ACCENT)
        self.cell(0, 10, title, 0, 1, 'L')
        if subtitle:
            self.set_font('Arial', '', 14)
            self.set_text_color(*COLOR_TEXT)
            self.cell(0, 10, subtitle, 0, 1, 'L')
        self.ln(10)

    def chapter_body(self, body):
        self.set_font('Arial', '', 11)
        self.set_text_color(*COLOR_TEXT)
        self.multi_cell(0, 8, body)
        self.ln()

    def add_card(self, title, content):
        self.set_fill_color(30, 30, 30)
        self.set_draw_color(*COLOR_ACCENT)
        self.set_line_width(0.5)
        self.rect(self.get_x(), self.get_y(), 190, 35, 'DF')
        
        self.set_xy(self.get_x() + 5, self.get_y() + 5)
        self.set_font('Arial', 'B', 12)
        self.set_text_color(*COLOR_ACCENT)
        self.cell(0, 0, title)
        
        self.set_xy(self.get_x(), self.get_y() + 8)
        self.set_font('Arial', '', 10)
        self.set_text_color(*COLOR_TEXT)
        self.multi_cell(180, 5, content)
        self.ln(15)

def create_brochure():
    print("🖨️ MAQUETANDO FOLLETO COMERCIAL...")
    
    # 1. Crear assets visuales
    cover_img = f"{OUTPUT_DIR}/cover_art.png"
    create_hex_art(cover_img)
    
    pdf = PDF()
    pdf.set_auto_page_break(auto=True, margin=15)

    # --- PÁGINA 1: PORTADA ---
    pdf.add_page()
    
    # Imagen central
    pdf.image(cover_img, x=10, y=60, w=190)
    
    pdf.set_y(180)
    pdf.set_font('Arial', 'B', 36)
    pdf.set_text_color(*COLOR_TEXT)
    pdf.multi_cell(0, 15, "RETAIL GENOME\nTHE SOVEREIGN ENGINE", align='C')
    
    pdf.ln(10)
    pdf.set_font('Arial', '', 14)
    pdf.set_text_color(*COLOR_ACCENT)
    pdf.cell(0, 10, "Reducimos la incertidumbre de tu expansión.", 0, 1, 'C')

    # --- PÁGINA 2: EL PROBLEMA ---
    pdf.add_page()
    pdf.chapter_title("EL PROBLEMA", "¿Por qué fallan las aperturas?")
    
    pdf.chapter_body(
        "Abrir una tienda fisica es la inversion mas grande de tu marca este año. "
        "Sin embargo, el 30% de las decisiones se toman basandose en intuicion o datos censales obsoletos.\n\n"
        "El coste de equivocarse no es solo el alquiler. Es la obra, el personal, el stock y el daño a la marca. "
        "No dejes que tu CAPEX dependa de la suerte."
    )
    
    pdf.ln(10)
    pdf.set_font('Arial', 'B', 14)
    pdf.cell(0, 10, "NUESTRA SOLUCION: GEMELOS DIGITALES", 0, 1)
    pdf.set_font('Arial', '', 11)
    pdf.multi_cell(0, 8, 
        "En Spatia Consulting no vendemos mapas. Convertimos datos masivos en decisiones binarias: GO / NO-GO.\n"
        "Nuestro algoritmo analiza el ADN de tus tiendas mas exitosas y busca secuencias geneticas identicas en nuevas ciudades."
    )

    # --- PÁGINA 3: LA TECNOLOGÍA ---
    pdf.add_page()
    pdf.chapter_title("THE INTELLIGENCE STACK", "4 Capas de Datos. 1 Decision.")
    
    pdf.add_card("1. CAPA FISICA (Accesibilidad Real)", 
                 "Calculamos isocronas de caminata real minuto a minuto con OSRM. Si hay una barrera fisica, nuestro modelo lo sabe.")
    
    pdf.add_card("2. CAPA ECONOMICA (Pocket Share)", 
                 "Cruzamos datos oficiales de Renta (INE) con micro-mallas H3. Identificamos donde esta el poder de compra real.")

    pdf.add_card("3. CAPA DEMOGRAFICA (Target Filter)", 
                 "Filtramos el ruido. Usamos satelites para contar SOLO a tu publico objetivo (15-35 años).")

    pdf.add_card("4. CAPA DE MAGNETISMO (Vitalidad)", 
                 "Detectamos 'Locomotoras' (Zara, Starbucks). Si ellos validan la zona, nosotros la puntuamos.")

    # --- PÁGINA 4: EL ENTREGABLE ---
    pdf.add_page()
    pdf.chapter_title("TU INVESTMENT MEMO", "Lo que recibes.")
    
    pdf.chapter_body(
        "No entregamos un login. Trabajamos como tus socios estrategicos para entregarte un Dossier Ejecutivo completo:"
    )
    pdf.ln(5)
    
    items = [
        "-> EL RANKING: Las 3 unicas ubicaciones que merecen tu dinero.",
        "-> EL ARBITRAJE: Grafica de Calidad vs. Precio (Gemas Ocultas).",
        "-> LA VALIDACION: Analisis de Canibalizacion con tiendas propias.",
        "-> EL VEREDICTO: Recomendacion clara de inversion respaldada por IA."
    ]
    
    for item in items:
        pdf.cell(10)
        pdf.cell(0, 10, item, 0, 1)

    # --- CONTRAPORTADA ---
    pdf.add_page()
    pdf.set_y(100)
    pdf.set_font('Arial', 'B', 24)
    pdf.set_text_color(*COLOR_ACCENT)
    pdf.cell(0, 10, "Deja de adivinar.", 0, 1, 'C')
    pdf.set_text_color(*COLOR_TEXT)
    pdf.cell(0, 10, "Empieza a saber.", 0, 1, 'C')
    
    pdf.set_y(240)
    pdf.set_font('Arial', '', 12)
    pdf.cell(0, 10, "www.spatiaconsulting.com", 0, 1, 'C')
    pdf.cell(0, 10, "Jesus Luna | Retail Strategy", 0, 1, 'C')

    # Guardar PDF
    pdf_file = f"{OUTPUT_DIR}/Spatia_Brochure_MVP.pdf"
    pdf.output(pdf_file)
    print(f"✅ ¡FOLLETO CREADO! Abre: {pdf_file}")

if __name__ == "__main__":
    create_brochure()