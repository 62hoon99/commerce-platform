package com.commerce.product.api;

import com.commerce.product.application.ProductService;
import com.commerce.product.application.RankingService;
import com.commerce.product.application.RankingService.RankingItem;
import com.commerce.product.domain.Product;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
@RequestMapping("/api/products")
public class ProductController {

    private final ProductService productService;
    private final RankingService rankingService;

    public ProductController(ProductService productService, RankingService rankingService) {
        this.productService = productService;
        this.rankingService = rankingService;
    }

    @GetMapping("/{productId}")
    public ResponseEntity<ProductResponse> getProduct(@PathVariable Long productId) {
        Product product = productService.getProduct(productId);
        return ResponseEntity.ok(ProductResponse.from(product));
    }

    @GetMapping("/ranking")
    public ResponseEntity<List<RankingItem>> getRanking() {
        return ResponseEntity.ok(rankingService.getTopRanking(10));
    }

    public record ProductResponse(Long id, String name, String category, BigDecimal price) {
        public static ProductResponse from(Product product) {
            return new ProductResponse(product.getId(), product.getName(), product.getCategory(), product.getPrice());
        }
    }
}
