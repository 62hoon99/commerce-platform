package com.commerce.product.application;

import com.commerce.product.domain.Product;
import com.commerce.product.domain.ProductRepository;
import org.springframework.stereotype.Service;

@Service
public class ProductService {

    private final ProductRepository productRepository;
    private final RankingService rankingService;

    public ProductService(ProductRepository productRepository, RankingService rankingService) {
        this.productRepository = productRepository;
        this.rankingService = rankingService;
    }

    public Product getProduct(Long productId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new IllegalArgumentException("Product not found: " + productId));
        rankingService.incrementViewScore(productId);
        return product;
    }
}
