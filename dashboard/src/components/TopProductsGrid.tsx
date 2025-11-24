import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Trophy, TrendingUp } from "lucide-react";

interface Product {
  product_id: number;
  frecuencia_absoluta: number;
  frecuencia_relativa_pct: number;
}

interface TopProductsGridProps {
  products: Product[];
  title?: string;
}

export const TopProductsGrid = ({ products, title = "Top Productos Más Vendidos" }: TopProductsGridProps) => {
  return (
    <Card className="animate-slide-up">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Trophy className="h-5 w-5 text-primary" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          {products.slice(0, 10).map((product, index) => (
            <div
              key={product.product_id}
              className="flex flex-col items-center p-4 rounded-lg bg-gradient-to-br from-muted/50 to-muted/30 hover:shadow-md transition-all duration-300 hover:scale-105"
            >
              <div className="relative mb-2">
                {index < 3 && (
                  <Badge
                    variant="default"
                    className={cn(
                      "absolute -top-2 -right-2 h-6 w-6 flex items-center justify-center p-0",
                      index === 0 && "bg-primary",
                      index === 1 && "bg-secondary",
                      index === 2 && "bg-accent"
                    )}
                  >
                    {index + 1}
                  </Badge>
                )}
                <div className="h-12 w-12 rounded-full bg-gradient-sunset flex items-center justify-center text-white font-bold text-lg">
                  {product.product_id}
                </div>
              </div>
              <div className="text-center">
                <p className="text-xs text-muted-foreground mb-1">Producto #{product.product_id}</p>
                <p className="font-bold text-lg">{product.frecuencia_absoluta.toLocaleString()}</p>
                <div className="flex items-center justify-center gap-1 mt-1">
                  <TrendingUp className="h-3 w-3 text-success" />
                  <span className="text-xs text-muted-foreground">{product.frecuencia_relativa_pct}%</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
};

function cn(...inputs: (string | undefined)[]) {
  return inputs.filter(Boolean).join(" ");
}
