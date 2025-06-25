"""Test ProductItem model functionality."""

from src.models import ProductItem


def test_product_item_creation():
    """Test basic ProductItem creation and properties."""
    item = ProductItem("ITEM001", "Test Product Description")
    
    assert item.group == "ZZZZ", "Group should always be ZZZZ"
    assert item.item == "ITEM001", "Item ID should be preserved"
    assert item.description == "Test Product Description", "Description should be preserved"
    assert item.is_valid(), "Item with ID and description should be valid"


def test_product_item_csv_output():
    """Test CSV row generation with proper quoting."""
    item = ProductItem("ITEM001", "Test Product")
    csv_row = item.to_csv_row()
    
    expected = ['"ZZZZ"', '"ITEM001"', '"Test Product"']
    assert csv_row == expected, f"CSV row should match expected format: {csv_row}"


def test_product_item_csv_headers():
    """Test CSV headers are correctly formatted."""
    headers = ProductItem.csv_headers()
    expected = ['"group"', '"item"', '"description"']
    assert headers == expected, f"Headers should match expected format: {headers}"


def test_product_item_quote_escaping():
    """Test that internal quotes are properly escaped."""
    item = ProductItem("ITEM001", 'Product with "quotes" in description')
    csv_row = item.to_csv_row()
    
    # The description should have internal quotes doubled
    expected_description = '"Product with ""quotes"" in description"'
    assert csv_row[2] == expected_description, f"Quotes should be escaped: {csv_row[2]}"


def test_product_item_validation():
    """Test ProductItem validation logic."""
    # Valid item
    valid_item = ProductItem("ITEM001", "Valid Product")
    assert valid_item.is_valid(), "Item with ID and description should be valid"
    
    # Invalid items
    empty_id = ProductItem("", "Has description")
    assert not empty_id.is_valid(), "Item with empty ID should be invalid"
    
    empty_desc = ProductItem("ITEM001", "")
    assert not empty_desc.is_valid(), "Item with empty description should be invalid"
    
    both_empty = ProductItem("", "")
    assert not both_empty.is_valid(), "Item with both empty should be invalid"


def test_product_item_whitespace_handling():
    """Test that whitespace is properly stripped."""
    item = ProductItem("  ITEM001  ", "  Product Description  ")
    
    assert item.item == "ITEM001", "Item ID whitespace should be stripped"
    assert item.description == "Product Description", "Description whitespace should be stripped"


def test_product_item_deduplication():
    """Test that ProductItems can be deduplicated by item ID."""
    item1 = ProductItem("ITEM001", "First Description")
    item2 = ProductItem("ITEM001", "Second Description")  # Same ID, different description
    item3 = ProductItem("ITEM002", "Third Description")
    
    # Test equality (for deduplication)
    assert item1 == item2, "Items with same ID should be equal"
    assert item1 != item3, "Items with different IDs should not be equal"
    
    # Test hashing (for set-based deduplication)
    assert hash(item1) == hash(item2), "Items with same ID should have same hash"
    assert hash(item1) != hash(item3), "Items with different IDs should have different hashes"
    
    # Test set deduplication
    items = [item1, item2, item3]
    unique_items = list(set(items))
    
    assert len(unique_items) == 2, "Should have 2 unique items after deduplication"
    unique_ids = {item.item for item in unique_items}
    assert unique_ids == {"ITEM001", "ITEM002"}, "Should preserve unique IDs"


def test_product_item_repr():
    """Test string representation for debugging."""
    item = ProductItem("ITEM001", "This is a long product description that will be truncated")
    repr_str = repr(item)
    
    assert "ITEM001" in repr_str, "Repr should contain item ID"
    assert "ProductItem" in repr_str, "Repr should contain class name"


if __name__ == "__main__":
    test_product_item_creation()
    test_product_item_csv_output()
    test_product_item_csv_headers()
    test_product_item_quote_escaping()
    test_product_item_validation()
    test_product_item_whitespace_handling()
    test_product_item_deduplication()
    test_product_item_repr()
    print("✅ All ProductItem tests passed!") 