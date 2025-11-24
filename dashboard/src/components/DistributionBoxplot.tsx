import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { TrendingUp, Users, ShoppingBag } from "lucide-react";
import { useState } from "react";

interface CustomerData {
  customer_id: number;
  frecuencia_absoluta: number;
  frecuencia_relativa_pct: number;
}

interface CategoryData {
  category_id: number | null;
  category_name: string | null;
  unidades_vendidas: number;
  porcentaje: number;
}

interface DistributionBoxplotProps {
  customers?: CustomerData[];
  categories?: CategoryData[];
  title?: string;
}

// Calcular estadísticas para boxplot
const calculateBoxplotStats = (values: number[]) => {
  const sorted = [...values].sort((a, b) => a - b);
  const q1Index = Math.floor(sorted.length * 0.25);
  const medianIndex = Math.floor(sorted.length * 0.5);
  const q3Index = Math.floor(sorted.length * 0.75);
  
  const q1 = sorted[q1Index] || 0;
  const median = sorted[medianIndex] || 0;
  const q3 = sorted[q3Index] || 0;
  const min = sorted[0] || 0;
  const max = sorted[sorted.length - 1] || 0;
  const iqr = q3 - q1;
  const lowerWhisker = Math.max(min, q1 - 1.5 * iqr);
  const upperWhisker = Math.min(max, q3 + 1.5 * iqr);
  
  return {
    min,
    q1,
    median,
    q3,
    max,
    lowerWhisker,
    upperWhisker,
    iqr,
    outliers: values.filter(v => v < lowerWhisker || v > upperWhisker)
  };
};

export const DistributionBoxplot = ({ 
  customers = [],
  categories = [],
  title = "Distribución de Totales" 
}: DistributionBoxplotProps) => {
  const [selectedView, setSelectedView] = useState<"customers" | "categories">(
    customers.length > 0 ? "customers" : "categories"
  );

  // Preparar datos según la vista seleccionada
  const getData = () => {
    if (selectedView === "customers" && customers.length > 0) {
      const values = customers.map(c => c.frecuencia_absoluta);
      const stats = calculateBoxplotStats(values);
      
      return {
        data: customers.map(c => ({
          id: `Cliente ${c.customer_id}`,
          value: c.frecuencia_absoluta,
          label: `#${c.customer_id}`,
          isOutlier: stats.outliers.includes(c.frecuencia_absoluta)
        })),
        stats,
        label: "Número de Compras",
        icon: Users
      };
    } else if (selectedView === "categories" && categories.length > 0) {
      const values = categories.map(c => c.unidades_vendidas);
      const stats = calculateBoxplotStats(values);
      
      return {
        data: categories.map(c => ({
          id: c.category_name || "Sin categoría",
          value: c.unidades_vendidas,
          label: (c.category_name || "Sin categoría").substring(0, 20),
          isOutlier: stats.outliers.includes(c.unidades_vendidas)
        })),
        stats,
        label: "Unidades Vendidas",
        icon: ShoppingBag
      };
    }
    return null;
  };

  const chartData = getData();

  if (!chartData) {
    return (
      <Card className="animate-slide-up">
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-primary" />
            {title}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-muted-foreground text-center py-8">
            No hay datos disponibles para mostrar
          </p>
        </CardContent>
      </Card>
    );
  }

  const { data, stats, label, icon: Icon } = chartData;

  // Crear datos para visualización tipo boxplot usando barras
  const boxplotData = [
    {
      name: "Distribución",
      min: stats.min,
      q1: stats.q1,
      median: stats.median,
      q3: stats.q3,
      max: stats.max,
      lowerWhisker: stats.lowerWhisker,
      upperWhisker: stats.upperWhisker
    }
  ];

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Icon className="h-5 w-5 text-primary" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {/* Selector de vista */}
          {(customers.length > 0 && categories.length > 0) && (
            <div className="flex gap-2">
              <button
                onClick={() => setSelectedView("customers")}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  selectedView === "customers"
                    ? "bg-primary text-primary-foreground"
                    : "bg-muted text-muted-foreground hover:bg-muted/80"
                }`}
              >
                Clientes (Top {customers.length})
              </button>
              <button
                onClick={() => setSelectedView("categories")}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                  selectedView === "categories"
                    ? "bg-primary text-primary-foreground"
                    : "bg-muted text-muted-foreground hover:bg-muted/80"
                }`}
              >
                Categorías (Top {categories.length})
              </button>
            </div>
          )}

          {/* Estadísticas descriptivas */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-2 text-xs">
            <div className="p-2 bg-muted/50 rounded text-center">
              <div className="text-muted-foreground">Mínimo</div>
              <div className="font-bold">{stats.min.toLocaleString()}</div>
            </div>
            <div className="p-2 bg-muted/50 rounded text-center">
              <div className="text-muted-foreground">Q1</div>
              <div className="font-bold">{Math.round(stats.q1).toLocaleString()}</div>
            </div>
            <div className="p-2 bg-primary/10 rounded text-center border border-primary/20">
              <div className="text-muted-foreground">Mediana</div>
              <div className="font-bold text-primary">{Math.round(stats.median).toLocaleString()}</div>
            </div>
            <div className="p-2 bg-muted/50 rounded text-center">
              <div className="text-muted-foreground">Q3</div>
              <div className="font-bold">{Math.round(stats.q3).toLocaleString()}</div>
            </div>
            <div className="p-2 bg-muted/50 rounded text-center">
              <div className="text-muted-foreground">Máximo</div>
              <div className="font-bold">{stats.max.toLocaleString()}</div>
            </div>
          </div>

          {/* Gráfico de distribución (barras horizontales) */}
          <div>
            <h4 className="text-sm font-medium mb-2">{label} - Distribución</h4>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={data} layout="vertical" margin={{ top: 5, right: 30, left: 100, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                <XAxis 
                  type="number"
                  stroke="hsl(var(--foreground))"
                  style={{ fontSize: '12px' }}
                  tickFormatter={(value) => value.toLocaleString()}
                />
                <YAxis 
                  type="category"
                  dataKey="label"
                  stroke="hsl(var(--foreground))"
                  style={{ fontSize: '11px' }}
                  width={90}
                />
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'hsl(var(--card))',
                    border: '1px solid hsl(var(--border))',
                    borderRadius: '8px',
                  }}
                  formatter={(value: number) => [
                    `${value.toLocaleString()} ${selectedView === "customers" ? "compras" : "unidades"}`,
                    label
                  ]}
                  labelFormatter={(label) => `ID: ${label}`}
                />
                <Legend />
                <Bar 
                  dataKey="value" 
                  radius={[0, 4, 4, 0]}
                  fill={selectedView === "customers" ? "hsl(var(--primary))" : "hsl(var(--accent))"}
                >
                  {data.map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={entry.isOutlier 
                        ? "hsl(var(--destructive))" 
                        : selectedView === "customers" 
                          ? "hsl(var(--primary))" 
                          : "hsl(var(--accent))"
                      } 
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Información de outliers */}
          {stats.outliers.length > 0 && (
            <div className="text-xs text-muted-foreground p-2 bg-destructive/10 rounded border border-destructive/20">
              <p className="font-medium text-destructive mb-1">
                ⚠️ Outliers detectados: {stats.outliers.length}
              </p>
              <p>
                Valores que están fuera del rango intercuartil (Q1 - 1.5×IQR, Q3 + 1.5×IQR) 
                se muestran en rojo. IQR = {Math.round(stats.iqr).toLocaleString()}
              </p>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
};


