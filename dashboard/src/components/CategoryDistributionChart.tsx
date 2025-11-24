import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { Package } from "lucide-react";

interface CategoryData {
  codigo_categoria: number;
  nombre_categoria: string;
  num_productos: number;
  porcentaje: number;
}

interface CategoryDistributionChartProps {
  data: CategoryData[];
  title?: string;
}

export const CategoryDistributionChart = ({ 
  data, 
  title = "Distribución de Productos por Categoría" 
}: CategoryDistributionChartProps) => {
  const topCategories = data.slice(0, 15).map(item => ({
    categoria: item.nombre_categoria.length > 20 
      ? item.nombre_categoria.substring(0, 20) + "..." 
      : item.nombre_categoria,
    productos: item.num_productos,
    porcentaje: item.porcentaje,
    fullName: item.nombre_categoria
  }));

  const colors = [
    "hsl(var(--primary))",
    "hsl(var(--secondary))",
    "hsl(var(--accent))",
    "hsl(var(--success))",
  ];

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Package className="h-5 w-5 text-primary" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={topCategories} layout="vertical">
            <defs>
              {colors.map((color, index) => (
                <linearGradient key={index} id={`categoryGradient-${index}`} x1="0" y1="0" x2="1" y2="0">
                  <stop offset="5%" stopColor={color} stopOpacity={0.9}/>
                  <stop offset="95%" stopColor={color} stopOpacity={0.6}/>
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              type="number"
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
            />
            <YAxis 
              type="category"
              dataKey="categoria"
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '11px' }}
              width={150}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
              }}
              formatter={(value: number, name: string, props: any) => [
                `${value.toLocaleString()} productos (${props.payload.porcentaje}%)`,
                'Productos'
              ]}
              labelFormatter={(label, payload) => payload?.[0]?.payload?.fullName || label}
            />
            <Legend />
            <Bar 
              dataKey="productos" 
              radius={[0, 8, 8, 0]}
              animationDuration={800}
            >
              {topCategories.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={`url(#categoryGradient-${index % colors.length})`} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};


