#!/usr/bin/env python
"""
An example of a step using MLflow and Weights & Biases]: Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import panda

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(project="nyc_aribnb", job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Starting download the artifact with name {0}".format(args.input_artifact))
    local_path = wandb.use_artifact("{0}:latest".format(args.input_artifact)) 
    logger.info("Artifact with name {0} has been downloaded".format(args.input_artifact))

    logger.info("Starting reading the file and extracting its data frame content")
    df = pd.read_csv(local_path)
    logger.info("CSV file has been read and assigned to a data frame variable")

    # get rid of outliers
    logger.info("Starting ged rid of outliers process")
    min_price, max_price = args.min_price, args.max_price
    remaining = df['price'].between(min_price, max_price)
    df = df[remaining].copy()
    logger.info("Get rid ouf outliers finished.")
    
    # save it a csv
    logger.info("Starting saving the data frame to csv.")
    df.to_csv(fileName, index=False)
    logger.info("Data frame has been saved to csv file.")

    # upload the data to wandb
    artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
    )
    artifact.add_file(fileName)
    run.log_artifact(artifact)

    # finish the run
    run.finish()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="A description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Max maximum price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
