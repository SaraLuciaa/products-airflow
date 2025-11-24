import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Users } from "lucide-react";

interface RetentionData {
  cohort_month: string;
  cohort_index: number;
  retention_rate: number;
}

interface CohortHeatmapProps {
  data: RetentionData[];
  title?: string;
}

export const CohortHeatmap = ({ data, title = "Análisis de Retención de Clientes" }: CohortHeatmapProps) => {
  // Group by cohort_month
  const cohorts = data.reduce((acc, item) => {
    if (!acc[item.cohort_month]) {
      acc[item.cohort_month] = [];
    }
    acc[item.cohort_month].push(item);
    return acc;
  }, {} as Record<string, RetentionData[]>);

  const cohortMonths = Object.keys(cohorts).sort().slice(0, 6);
  const maxIndex = Math.max(...data.map(d => d.cohort_index));

  const getColorForRetention = (rate: number) => {
    if (rate >= 0.8) return "bg-success/80";
    if (rate >= 0.6) return "bg-primary/60";
    if (rate >= 0.4) return "bg-accent/60";
    if (rate >= 0.2) return "bg-secondary/60";
    return "bg-muted";
  };

  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Users className="h-5 w-5 text-success" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className="text-xs font-medium text-muted-foreground p-2 text-left">Cohorte</th>
                {Array.from({ length: Math.min(maxIndex + 1, 6) }, (_, i) => (
                  <th key={i} className="text-xs font-medium text-muted-foreground p-2 text-center">
                    Mes {i}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {cohortMonths.map(month => {
                const cohortData = cohorts[month].sort((a, b) => a.cohort_index - b.cohort_index);
                return (
                  <tr key={month} className="border-t border-border">
                    <td className="text-xs font-medium p-2">
                      {new Date(month).toLocaleDateString('es-CO', { month: 'short', year: '2-digit' })}
                    </td>
                    {Array.from({ length: Math.min(maxIndex + 1, 6) }, (_, i) => {
                      const dataPoint = cohortData.find(d => d.cohort_index === i);
                      return (
                        <td key={i} className="p-1">
                          {dataPoint ? (
                            <div
                              className={`${getColorForRetention(dataPoint.retention_rate)} rounded p-2 text-center text-xs font-semibold transition-all hover:scale-105`}
                              title={`${(dataPoint.retention_rate * 100).toFixed(1)}%`}
                            >
                              {(dataPoint.retention_rate * 100).toFixed(0)}%
                            </div>
                          ) : (
                            <div className="bg-muted/30 rounded p-2 text-center text-xs">-</div>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <div className="flex items-center gap-4 mt-4 text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-muted"></div>
            <span>&lt;20%</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-secondary/60"></div>
            <span>20-40%</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-accent/60"></div>
            <span>40-60%</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-primary/60"></div>
            <span>60-80%</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-4 h-4 rounded bg-success/80"></div>
            <span>&gt;80%</span>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
