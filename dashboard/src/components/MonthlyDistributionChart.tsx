import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";
import { Calendar } from "lucide-react";

interface MonthlyData {
  year: number;
  month: number;
  num_transacciones: number;
  porcentaje: number;
}

interface MonthlyDistributionChartProps {
  data: MonthlyData[];
  title?: string;
}

const monthNames = [
  "Ene", "Feb", "Mar", "Abr", "May", "Jun",
  "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"
];

export const MonthlyDistributionChart = ({ 
  data, 
  title = "Distribución Mensual de Transacciones" 
}: MonthlyDistributionChartProps) => {
  const chartData = data.map(item => ({
    mes: `${monthNames[item.month - 1]} ${item.year}`,
    transacciones: item.num_transacciones,
    porcentaje: item.porcentaje,
  }));

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Calendar className="h-5 w-5 text-primary" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={chartData}>
            <defs>
              <linearGradient id="monthlyGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.8}/>
                <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0.1}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis 
              dataKey="mes" 
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
            />
            <YAxis 
              stroke="hsl(var(--foreground))"
              style={{ fontSize: '12px' }}
              tickFormatter={(value) => `${(value / 1000).toFixed(0)}K`}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px',
              }}
              formatter={(value: number, name: string, props: any) => [
                `${value.toLocaleString()} (${props.payload.porcentaje}%)`,
                'Transacciones'
              ]}
            />
            <Legend />
            <Line
              type="monotone"
              dataKey="transacciones"
              stroke="hsl(var(--primary))"
              strokeWidth={3}
              dot={{ fill: 'hsl(var(--primary))', strokeWidth: 2, r: 5 }}
              activeDot={{ r: 8 }}
              fill="url(#monthlyGradient)"
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
};


