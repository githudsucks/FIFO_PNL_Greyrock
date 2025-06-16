import pandas as pd
from collections import defaultdict

def calculate_fifo_pnl(csv_file):
    """
    Calculate FIFO PnL from a CSV file containing bond trades.
    
    Args:
        csv_file: Path to CSV file with columns ['Contract', 'Price', 'Quantity']
        
    Returns:
        Tuple of (realized_pnl, remaining_positions_df)
    """
    # Read CSV, handling potential whitespace in Contract names
    df = pd.read_csv(csv_file)
    df['Contract'] = df['Contract'].str.strip()
    
    positions = defaultdict(list)  # {contract: [(price, quantity), ...]}
    realized_pnl = 0.0
    trade_history = []
    
    for _, row in df.iterrows():
        contract = row['Contract']
        price = row['Price']
        quantity = row['Quantity']
        
        if quantity > 0:  # Buy
            positions[contract].append((price, quantity))
            trade_history.append({
                'Type': 'BUY',
                'Contract': contract,
                'Price': price,
                'Quantity': quantity,
                'PnL': 0,
                'Remaining': f"{contract} {quantity} @ {price}"
            })
        else:  # Sell (negative quantity)
            remaining_sell_quantity = -quantity
            original_sell_qty = remaining_sell_quantity
            
            while remaining_sell_quantity > 0 and positions.get(contract):
                first_buy_price, first_buy_quantity = positions[contract][0]
                
                if first_buy_quantity <= remaining_sell_quantity:
                    # Entire position is closed
                    lot_pnl = (price - first_buy_price) * first_buy_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'SELL',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': first_buy_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Matched {first_buy_quantity} @ {first_buy_price}"
                    })
                    remaining_sell_quantity -= first_buy_quantity
                    positions[contract].pop(0)
                else:
                    # Partial position is closed
                    lot_pnl = (price - first_buy_price) * remaining_sell_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'SELL',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': remaining_sell_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Matched {remaining_sell_quantity} @ {first_buy_price}"
                    })
                    positions[contract][0] = (
                        first_buy_price, 
                        first_buy_quantity - remaining_sell_quantity
                    )
                    remaining_sell_quantity = 0
    
    # Prepare remaining positions report
    remaining_data = []
    for contract, lots in positions.items():
        for price, quantity in lots:
            remaining_data.append({
                'Contract': contract,
                'Price': price,
                'Quantity': quantity
            })
    
    remaining_positions_df = pd.DataFrame(remaining_data)
    
    # Prepare trade history report
    trade_history_df = pd.DataFrame(trade_history)
    
    return realized_pnl, remaining_positions_df, trade_history_df

def main():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python fifo_pnl_calculator.py <input_csv_file>")
        return
    
    input_file = sys.argv[1]
    
    try:
        pnl, remaining_positions, trade_history = calculate_fifo_pnl(input_file)
        
        print("\n=== FIFO PnL Calculation Results ===")
        print(f"\nTotal Realized PnL: ${pnl:.2f}")
        
        print("\n=== Trade History ===")
        print(trade_history.to_string(index=False))
        
        print("\n=== Remaining Positions ===")
        if not remaining_positions.empty:
            # Calculate average price per contract
            avg_prices = remaining_positions.groupby('Contract').apply(
                lambda x: sum(x['Price'] * x['Quantity']) / sum(x['Quantity'])
            ).reset_index(name='AvgPrice')
            
            totals = remaining_positions.groupby('Contract')['Quantity'].sum().reset_index()
            remaining_report = pd.merge(totals, avg_prices, on='Contract')
            
            print(remaining_report.to_string(index=False))
            
            print("\nDetailed Lots:")
            for contract in remaining_positions['Contract'].unique():
                print(f"\n{contract}:")
                contract_lots = remaining_positions[remaining_positions['Contract'] == contract]
                print(contract_lots.to_string(index=False))
        else:
            print("No remaining positions")
            
        # Generate output files
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        trade_history.to_csv(f"trade_history_{timestamp}.csv", index=False)
        remaining_positions.to_csv(f"remaining_positions_{timestamp}.csv", index=False)
        print(f"\nReports saved as trade_history_{timestamp}.csv and remaining_positions_{timestamp}.csv")
        
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
