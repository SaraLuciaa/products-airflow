import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";
import { Store } from "lucide-react";

interface StoreData {
  store_id: number;
  num_transacciones: number;
  porcentaje: number;
}

interface StoreDistributionChartProps {
  data: StoreData[];
  title?: string;
}

const COLORS = [
  "hsl(var(--primary))",
  "hsl(var(--secondary))",
  "hsl(var(--accent))",
  "hsl(var(--success))",
];

export const StoreDistributionChart = ({ 
  data, 
  title = "Distribución de Transacciones por Tienda" 
}: StoreDistributionChartProps) => {
  const chartData = data.map((item, index) => ({
    name: `Tienda ${item.store_id}`,
    value: item.num_transacciones,
    porcentaje: item.porcentaje,
    color: COLORS[index % COLORS.length]
  }));

  const renderCustomLabel = (entry: any) => {
    return `${entry.porcentaje.toFixed(1)}%`;
  };

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Store className="h-5 w-5 text-accent" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              labelLine={false}
              label={renderCustomLabel}
              outerRadius={100}
              fill="#8884d8"
              dataKey="value"
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
              }}
              formatter={(value: number, name: string, props: any) => [
                `${value.toLocaleString()} transacciones (${props.payload.porcentaje.toFixed(2)}%)`,
                name
              ]}
            />
            <Legend 
              formatter={(value, entry: any) => (
                <span style={{ color: entry.color }}>
                  {value}: {entry.payload.porcentaje.toFixed(1)}%
                </span>
              )}
            />
          </PieChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};


