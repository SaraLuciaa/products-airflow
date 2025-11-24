import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Users, TrendingUp } from "lucide-react";

interface Customer {
  customer_id: number;
  frecuencia_absoluta: number;
  frecuencia_relativa_pct: number;
}

interface TopCustomersGridProps {
  customers: Customer[];
  title?: string;
}

export const TopCustomersGrid = ({ 
  customers, 
  title = "Top 10 Clientes Más Compras" 
}: TopCustomersGridProps) => {
  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Users className="h-5 w-5 text-accent" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          {customers.slice(0, 10).map((customer, index) => (
            <div
              key={customer.customer_id}
              className="flex flex-col items-center p-4 rounded-lg bg-gradient-to-br from-muted/50 to-muted/30 hover:shadow-md transition-all duration-300 hover:scale-105"
            >
              <div className="relative mb-2">
                {index < 3 && (
                  <Badge
                    variant="default"
                    className={`absolute -top-2 -right-2 h-6 w-6 flex items-center justify-center p-0 ${
                      index === 0 ? "bg-primary" :
                      index === 1 ? "bg-secondary" :
                      "bg-accent"
                    }`}
                  >
                    {index + 1}
                  </Badge>
                )}
                <div className="h-12 w-12 rounded-full bg-gradient-to-br from-primary to-accent flex items-center justify-center text-white font-bold text-lg">
                  {String(customer.customer_id).slice(-2)}
                </div>
              </div>
              <div className="text-center">
                <p className="text-xs text-muted-foreground mb-1">Cliente #{customer.customer_id}</p>
                <p className="font-bold text-lg">{customer.frecuencia_absoluta.toLocaleString()}</p>
                <div className="flex items-center justify-center gap-1 mt-1">
                  <TrendingUp className="h-3 w-3 text-success" />
                  <span className="text-xs text-muted-foreground">{customer.frecuencia_relativa_pct}%</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
};


