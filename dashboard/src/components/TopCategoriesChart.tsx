import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from "recharts";
import { ShoppingBag } from "lucide-react";

interface CategorySalesData {
  category_id: number | null;
  category_name: string | null;
  unidades_vendidas: number;
  porcentaje: number;
}

interface TopCategoriesChartProps {
  data: CategorySalesData[];
  totalUnidades?: number;
  title?: string;
}

export const TopCategoriesChart = ({ 
  data, 
  totalUnidades,
  title = "Top Categorías por Unidades Vendidas" 
}: TopCategoriesChartProps) => {
  const chartData = data.map(item => ({
    categoria: item.category_name || "Sin categoría",
    unidades: item.unidades_vendidas,
    porcentaje: item.porcentaje,
    shortName: (item.category_name || "Sin categoría").length > 25
      ? (item.category_name || "Sin categoría").substring(0, 25) + "..."
      : (item.category_name || "Sin categoría")
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
          <ShoppingBag className="h-5 w-5 text-success" />
          {title}
          {totalUnidades && (
            <span className="text-sm font-normal text-muted-foreground ml-2">
              ({totalUnidades.toLocaleString()} unidades totales)
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={chartData}>
            <defs>
              {colors.map((color, index) => (
                <linearGradient key={index} id={`categorySalesGradient-${index}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={color} stopOpacity={0.9}/>
                  <stop offset="95%" stopColor={color} stopOpacity={0.6}/>
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="shortName"
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '11px' }}
              angle={-45}
              textAnchor="end"
              height={100}
            />
            <YAxis 
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => `${(value / 1000000).toFixed(1)}M`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
              }}
              formatter={(value: number, name: string, props: any) => [
                `${value.toLocaleString()} unidades (${props.payload.porcentaje.toFixed(2)}%)`,
                'Unidades Vendidas'
              ]}
              labelFormatter={(label, payload) => payload?.[0]?.payload?.categoria || label}
            />
            <Legend />
            <Bar 
              dataKey="unidades" 
              radius={[8, 8, 0, 0]}
              animationDuration={800}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={`url(#categorySalesGradient-${index % colors.length})`} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};


